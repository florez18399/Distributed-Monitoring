from flask import Flask

def create_app():
    app = Flask(__name__)
    
    from app.controllers.trace_controller import trace_bp
    app.register_blueprint(trace_bp)
    
    return app