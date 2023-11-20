# from pydantic import BaseSettings
from pydantic_settings import BaseSettings, SettingsConfigDict


# class Settings(BaseSettings):
#     WEAVIATE_URL: str
#     WEAVIATE_API_KEY: str
#     OPENAI_API_KEY: str 
#     class Config:
#         env_file = ".env"

class Settings(BaseSettings):
    SUPABASE_URL: str
    SUPABASE_ANON_KEY: str
    SUPABASE_SERVICE_KEY: str
    WEAVIATE_URL: str
    WEAVIATE_API_KEY: str
    OPENAI_API_KEY: str
    KNOWLEDGE_SOURCE_CLASS: str = "KnowledgeSourceId_{}"
    CONTENT_CLASS: str = "ContentId_{}"
    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()