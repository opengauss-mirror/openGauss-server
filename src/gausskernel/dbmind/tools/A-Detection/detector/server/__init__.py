from .database import db
from .detection_app import MyApp


def start_service(config_path):
    app = MyApp()
    app.initialize_config(config_path)
    app.initialize_app()
    app.initialize_database(db)
    app.add_resources()
    app.start_service()
