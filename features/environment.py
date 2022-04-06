import os
import shutil
import stat
import subprocess
import tempfile
import venv
from pathlib import Path


def create_new_venv() -> Path:
    """Create a new venv.
    Returns:
        path to created venv
    """
    # Create venv
    venv_dir = Path(tempfile.mkdtemp())
    venv.main([str(venv_dir)])
    return venv_dir


def before_feature(context, feature):
    """Environment preparation before each test is run.
    """
    context.venv_dir = create_new_venv()
    bin_dir = context.venv_dir / "bin"
    context.pip = str(bin_dir / "pip")
    context.python = str(bin_dir / "python")
    context.kedro = str(bin_dir / "kedro")
    context.mkdir = "mkdir"
    context.starter_path = str(Path(__file__).parents[1])
    context.root_project_dir = Path.cwd()
    context.repo_name = "behave-test"
    context.package_name = "behave_test"
    subprocess.run(
        [context.python, "-m", "pip", "install", "-U", "pip", "wheel", "pypandoc"]
    )
    subprocess.run([context.pip, "install", "-r", "test_requirements.txt"])
    subprocess.run([context.pip, "install", "-U",
                    f'pyspark=={os.getenv("SPARK_VERSION", "2.4.0")}'])


def before_scenario(context, scenario):
    context.temp_dir = Path(tempfile.mkdtemp())


def after_feature(context, feature):
    rmtree(str(context.venv_dir))


def after_scenario(context, scenario):
    rmtree(str(context.temp_dir))


def rmtree(top):
    if os.name != "posix":
        for root, _, files in os.walk(top, topdown=False):
            for name in files:
                os.chmod(os.path.join(root, name), stat.S_IWUSR)
    shutil.rmtree(top)
