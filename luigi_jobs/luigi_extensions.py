import luigi
from utils import build_utils
from multiprocessing import Manager

# superclass that defines withConfig() method
class ConfigurableTask(luigi.Task):

    @classmethod
    def set_config(cls, job_config):
        if hasattr(ConfigurableTask, 'job'):
            raise AttributeError('Config has already been set')
        # Manager.dict() provides a proxy object which can be shared between multiple luigi processes
        cls.job = Manager().dict(job_config)