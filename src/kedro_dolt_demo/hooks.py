"""Project hooks."""
from typing import Any, Dict, Iterable, Optional
import logging
from dvc.repo import Repo
from dvc.repo.add import add
from dvc.repo.init import init
from dvc.repo.push import push
from kedro.config import ConfigLoader
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.versioning import Journal

logger = logging.getLogger(__name__)
class ProjectHooks:
    def __init__(self, data_location='data/02_intermediate/train_x.csv', remote_name='storage'):
        self.repo = None
        self.data_location = data_location
        self.remote_name = remote_name

    @hook_impl
    def before_pipeline_run(self):
        try:
            # ? Initializing DVC 
            init()
        except Exception:
            pass

    @hook_impl
    def after_pipeline_run(self, run_params: Dict[str, Any]):
        self.repo = Repo()
        
        logging.info(f'Running Command --> dvc add {self.data_location}')
        add(self.repo, targets=f'{str(self.repo)}/{self.data_location}')
        
        logging.info('Running Command --> dvc push')
        push(repo=self.repo, remote=self.remote_name)
        

    @hook_impl
    def register_config_loader(
        self, conf_paths: Iterable[str], env: str, extra_params: Dict[str, Any]
    ):
        return ConfigLoader(conf_paths)

    @hook_impl
    def register_catalog(
        self,
        catalog: Optional[Dict[str, Dict[str, Any]]],
        credentials: Dict[str, Dict[str, Any]],
        load_versions: Dict[str, str],
        save_version: str,
        journal: Journal,
    ):
        return DataCatalog.from_config(
            catalog, credentials, load_versions, save_version, journal
        )
