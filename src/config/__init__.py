import os
env = os.environ.get('PIPELINE_ENV', 'local')
if env == 'aws': from .aws import *
else: from .local import *