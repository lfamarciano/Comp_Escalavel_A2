from pathlib import Path

import sys
db_dir = Path(__file__).resolve().parent / 'db' # equivalente a .\db
sys.path.append(str(db_dir))
from db.db_config import DB_CONFIG, DB_NAME