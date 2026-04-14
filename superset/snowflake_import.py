import os

from superset import create_app

app = create_app()

snowflake_uri = (
    f"snowflake://{os.environ['SNOWFLAKE_USER']}:{os.environ['SNOWFLAKE_PASSWORD']}"
    f"@{os.environ['SNOWFLAKE_ACCOUNT']}/{os.environ['SNOWFLAKE_DATABASE']}"
    f"?warehouse={os.environ['SNOWFLAKE_WAREHOUSE']}"
)

with app.app_context():
    from superset.extensions import db
    from superset.models.core import Database

    existing = db.session.query(Database).filter_by(database_name="Snowflake LOL").first()
    if existing:
        print("Snowflake connection already exists, skipping.")
    else:
        database = Database(database_name="Snowflake LOL")
        database.set_sqlalchemy_uri(snowflake_uri)
        db.session.add(database)
        db.session.commit()
        print("Snowflake connection created successfully.")
