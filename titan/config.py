from pathlib import Path

from pydantic import AnyUrl, BaseSettings

from titan.logger import get_logger

logger = get_logger(__name__)


class MongoDns(AnyUrl):
    allowed_schemes = {"mongodb"}
    user_required = True


class _Settings(BaseSettings):
    # api settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8080
    API_DEBUG: bool = False
    API_KEY: str = "8ce654d9-0d68-4576-bad1-73794fa163f4"
    API_KEY_NAME: str = "access_token"

    # for applications sub-mounted below a given URL path
    ROOT_PATH: str = ""

    # database connection
    MONGO_DNS: MongoDns = "mongodb://root:root@localhost:27017"

    # ontology RDF repository connection
    RDF_REPOSITORY_ENDPOINT = "http://localhost:8086/sparql-auth/"
    RDF_REPOSITORY_USERNAME = "admin"
    RDF_REPOSITORY_PASSWORD = "password"
    RDF_REPOSITORY_DB = "default"

    # workflow orchestrator
    DRAMA_HOST: str = "localhost"
    DRAMA_PORT: int = 8081
    DRAMA_TOKEN: str = ""

    @property
    def rdf_connection_settings(self) -> dict:
        return {
            "database": self.RDF_REPOSITORY_DB,
            "endpoint": self.RDF_REPOSITORY_ENDPOINT,
            "username": self.RDF_REPOSITORY_USERNAME,
            "password": self.RDF_REPOSITORY_PASSWORD,
        }

    class Config:
        env_file = ".env"
        file_path = Path(env_file)
        if not file_path.is_file():
            logger.warning("⚠️ `.env` not found in current directory")
            logger.info("⚙️ Loading settings from environment")
        else:
            logger.info(f"⚙️ Loading settings from dotenv @ {file_path.absolute()}")


settings = _Settings()
