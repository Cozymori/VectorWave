# src/vectorwave/database/dataset.py
import logging
import uuid
import math
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import weaviate.classes.query as wvc_query
from weaviate.util import generate_uuid5

from .db import get_cached_client
from ..models.db_config import get_weaviate_settings

logger = logging.getLogger(__name__)


class VectorWaveDatasetManager:
    """
    Manages the 'VectorWaveGoldenDataset' collection.
    Provides interfaces for registering golden data and recommending candidates based on vector density.
    """

    def __init__(self):
        self.client = get_cached_client()
        self.settings = get_weaviate_settings()
        self.exec_col = self.client.collections.get(self.settings.EXECUTION_COLLECTION_NAME)
        self.golden_col = self.client.collections.get(self.settings.GOLDEN_COLLECTION_NAME)

    def register_as_golden(self, log_uuid: str, note: str = "", tags: List[str] = None) -> bool:
        """
        [Issue #78] Copies a specific execution log to the Golden Dataset.
        """
        try:
            # 1. Fetch original log (including vector)
            log_obj = self.exec_col.query.fetch_object_by_id(
                uuid=log_uuid,
                include_vector=True
            )

            if log_obj is None:
                logger.error(f"Log UUID '{log_uuid}' not found.")
                return False

            props = log_obj.properties

            # 2. Configure Golden Data properties
            golden_props = {
                "original_uuid": str(log_obj.uuid),
                "function_name": props.get("function_name"),
                "function_uuid": props.get("function_uuid"),
                "return_value": props.get("return_value"),
                "note": note,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "tags": tags if tags else []
            }

            # 3. Save (reuse original vector)
            vector = (log_obj.vector or {}).get("default")
            if vector is None:
                logger.error(
                    f"Log '{log_uuid}' has no stored vector. "
                    "The source function must have capture_return_value=True for vector storage."
                )
                return False

            self.golden_col.data.insert(
                properties=golden_props,
                vector=vector,
                uuid=generate_uuid5(log_uuid)  # Regenerate to avoid UUID collision, or maintain relation with original
            )
            logger.info(f"✅ Registered log {log_uuid} as Golden Data.")
            return True

        except Exception as e:
            logger.error(f"Failed to register golden data: {e}")
            return False

    def recommend_candidates(self, function_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        [Issue #80] Density-Based Recommendation Logic.
        Analyzes the vector distribution of existing Golden Data to suggest new candidates.
        """
        # 1. Fetch all vectors from Golden Data
        golden_objs = self.golden_col.query.fetch_objects(
            filters=wvc_query.Filter.by_property("function_name").equal(function_name),
            include_vector=True,
            limit=1000
        ).objects

        if not golden_objs:
            logger.info("No Golden Data found. Cannot calculate density.")
            return []

        # 2. Calculate Centroid and Density (Average Distance)
        vectors = [obj.vector["default"] for obj in golden_objs]
        if not vectors: return []

        dim = len(vectors[0])
        centroid = [sum(col) / len(vectors) for col in zip(*vectors)]

        # Calculate Euclidean distance between each vector and Centroid
        distances = []
        for v in vectors:
            dist = math.dist(v, centroid)
            distances.append(dist)

        avg_distance = sum(distances) / len(distances)  # This becomes the 'reference density'

        logger.info(f"[{function_name}] Golden Density (Avg Dist): {avg_distance:.4f}")

        # 3. Search candidates (successful cases from standard execution logs)
        # Exclude logs already registered as Golden (Filtering by original_uuid is complex, so handle in memory)
        candidates = self.exec_col.query.near_vector(
            near_vector=centroid,  # Fetch closest to centroid first
            limit=limit * 5,
            filters=(
                    wvc_query.Filter.by_property("function_name").equal(function_name) &
                    wvc_query.Filter.by_property("status").equal("SUCCESS")
            ),
            return_metadata=wvc_query.MetadataQuery(distance=True),
            include_vector=True
        ).objects

        golden_origin_ids = {obj.properties.get("original_uuid") for obj in golden_objs}

        recommendations = []

        # 4. Classification Logic (Steady vs Discovery)
        steady_limit = avg_distance + self.settings.RECOMMENDATION_STEADY_MARGIN
        discovery_limit = steady_limit + self.settings.RECOMMENDATION_DISCOVERY_MARGIN

        for cand in candidates:
            if str(cand.uuid) in golden_origin_ids:
                continue

            # Weaviate near_vector distance is usually Cosine Distance (0~2) or Euclidean
            # Here, use the distance calculated directly with the Centroid
            dist_to_centroid = math.dist(cand.vector["default"], centroid)

            rec_type = "IGNORE"
            if dist_to_centroid <= steady_limit:
                rec_type = "STEADY"  # Steady: Located within existing data distribution
            elif steady_limit < dist_to_centroid <= discovery_limit:
                rec_type = "DISCOVERY"  # Discovery: Slightly outside existing distribution (Potential new pattern)

            if rec_type != "IGNORE":
                recommendations.append({
                    "uuid": str(cand.uuid),
                    "type": rec_type,
                    "distance_to_center": dist_to_centroid,
                    "avg_density": avg_distance,
                    "return_value": cand.properties.get("return_value")
                })

            if len(recommendations) >= limit:
                break

        return recommendations
