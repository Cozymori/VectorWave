# src/vectorwave/database/dataset.py
import logging
import math
from datetime import datetime, timezone
from typing import List, Dict, Any

from weaviate.util import generate_uuid5

from ..models.db_config import get_weaviate_settings
from ..store import get_vector_store

logger = logging.getLogger(__name__)


class VectorWaveDatasetManager:
    """
    Manages the 'VectorWaveGoldenDataset' collection.
    Provides interfaces for registering golden data and recommending candidates based on vector density.

    Routes through the VectorStore abstraction so the same code path works in
    Pro (Weaviate) and Lite (LanceDB) modes.
    """

    def __init__(self):
        self.settings = get_weaviate_settings()
        self.store = get_vector_store()

    def register_as_golden(self, log_uuid: str, note: str = "", tags: List[str] = None) -> bool:
        """
        Copies a specific execution log to the Golden Dataset, preserving the
        original vector. Returns False if the source log is missing or has no
        stored vector (capture_return_value=True is required upstream for the
        log to carry one).
        """
        try:
            log_rec = self.store.fetch_by_id(
                collection=self.settings.EXECUTION_COLLECTION_NAME,
                uuid=log_uuid,
                include_vector=True,
            )
            if log_rec is None:
                logger.error(f"Log UUID '{log_uuid}' not found.")
                return False

            props = log_rec.properties
            vector = log_rec.vector
            if not vector:
                logger.error(
                    f"Log '{log_uuid}' has no stored vector. "
                    "The source function must have capture_return_value=True for vector storage."
                )
                return False

            golden_props = {
                "original_uuid": str(log_rec.uuid),
                "function_name": props.get("function_name"),
                "function_uuid": props.get("function_uuid"),
                "return_value": props.get("return_value"),
                "note": note,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "tags": tags if tags else [],
            }

            self.store.insert(
                collection=self.settings.GOLDEN_COLLECTION_NAME,
                properties=golden_props,
                vector=vector,
                uuid=generate_uuid5(log_uuid),
            )
            logger.info(f"✅ Registered log {log_uuid} as Golden Data.")
            return True

        except Exception as e:
            logger.error(f"Failed to register golden data: {e}")
            return False

    def recommend_candidates(self, function_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Density-Based Recommendation Logic.
        Analyzes the vector distribution of existing Golden Data to suggest new candidates.
        """
        # 1. Fetch all Golden entries for the function, then refetch with vectors.
        golden_records = self.store.query(
            collection=self.settings.GOLDEN_COLLECTION_NAME,
            filters={"function_name": function_name},
            limit=1000,
        )
        golden_with_vectors = []
        for rec in golden_records:
            full = self.store.fetch_by_id(
                self.settings.GOLDEN_COLLECTION_NAME, rec.uuid, include_vector=True
            )
            if full and full.vector:
                golden_with_vectors.append(full)

        if not golden_with_vectors:
            logger.info("No Golden Data found. Cannot calculate density.")
            return []

        # 2. Calculate Centroid and Density (Average Distance)
        vectors = [rec.vector for rec in golden_with_vectors]
        if not vectors:
            return []

        centroid = [sum(col) / len(vectors) for col in zip(*vectors)]
        distances = [math.dist(v, centroid) for v in vectors]
        avg_distance = sum(distances) / len(distances)

        logger.info(f"[{function_name}] Golden Density (Avg Dist): {avg_distance:.4f}")

        # 3. Candidates: SUCCESS executions for this function, near the centroid.
        candidates = self.store.near_vector(
            collection=self.settings.EXECUTION_COLLECTION_NAME,
            vector=centroid,
            filters={"function_name": function_name, "status": "SUCCESS"},
            limit=limit * 5,
            include_vector=True,
        )

        golden_origin_ids = {rec.properties.get("original_uuid") for rec in golden_with_vectors}

        recommendations: List[Dict[str, Any]] = []

        steady_limit = avg_distance + self.settings.RECOMMENDATION_STEADY_MARGIN
        discovery_limit = steady_limit + self.settings.RECOMMENDATION_DISCOVERY_MARGIN

        for cand in candidates:
            if cand.uuid in golden_origin_ids:
                continue

            cand_vec = cand.vector
            if cand_vec is None:
                continue
            dist_to_centroid = math.dist(cand_vec, centroid)

            rec_type = "IGNORE"
            if dist_to_centroid <= steady_limit:
                rec_type = "STEADY"
            elif steady_limit < dist_to_centroid <= discovery_limit:
                rec_type = "DISCOVERY"

            if rec_type != "IGNORE":
                recommendations.append({
                    "uuid": cand.uuid,
                    "type": rec_type,
                    "distance_to_center": dist_to_centroid,
                    "avg_density": avg_distance,
                    "return_value": cand.properties.get("return_value"),
                })

            if len(recommendations) >= limit:
                break

        return recommendations
