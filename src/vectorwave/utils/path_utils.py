import os
from typing import Optional, Tuple

def get_repo_root_and_relative_path(file_path: str) -> Optional[Tuple[str, str]]:
    """
    Finds the Git repository root and returns the file path relative to that root.

    Args:
        file_path (str): The absolute path to the file.

    Returns:
        Optional[Tuple[str, str]]: A tuple containing (repo_root_path, relative_file_path)
                                   if a Git repo is found, otherwise None.
    """
    if not os.path.isabs(file_path):
        # Ensure we're working with an absolute path
        file_path = os.path.abspath(file_path)

    current_dir = os.path.dirname(file_path)
    original_file_name = os.path.basename(file_path)

    while True:
        if os.path.exists(os.path.join(current_dir, '.git')):
            # Found the .git directory, so current_dir is the repo root
            repo_root = current_dir
            relative_path = os.path.relpath(file_path, repo_root)
            return repo_root, relative_path
        
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir: # Reached filesystem root
            return None
        current_dir = parent_dir
