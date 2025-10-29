import os
import sys
import inspect

def setup_project_paths(target_folder: str = "src"):
    """
    Ensures the project root and `src` folder are in sys.path,
    no matter where the script or notebook is executed from.

    Args:
        target_folder (str): Name of your source folder (default: "src")
    """
    # Detect the caller’s file or notebook working directory
    try:
        caller_frame = inspect.stack()[1]
        caller_file = caller_frame.filename
        base_path = os.path.dirname(os.path.abspath(caller_file))
    except Exception:
        # fallback for notebooks
        base_path = os.getcwd()

    # Find the project root (up until we find the target folder)
    current = base_path
    while current != os.path.dirname(current):  # stop at drive root
        if os.path.isdir(os.path.join(current, target_folder)):
            project_root = current
            break
        current = os.path.dirname(current)
    else:
        raise RuntimeError(f"Could not find '{target_folder}' folder above {base_path}")

    src_dir = os.path.join(project_root, target_folder)

    # Add both project root and src to sys.path if not present
    for path in [project_root, src_dir]:
        if path not in sys.path:
            sys.path.append(path)
    print("✅ Project paths set up successfully.")
    return project_root, src_dir
