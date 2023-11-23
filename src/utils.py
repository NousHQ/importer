from functools import lru_cache
import weaviate
from config import settings
from supabase import create_client

def get_weaviate_client():
    return weaviate.Client(
        url = settings.WEAVIATE_URL,
        auth_client_secret=weaviate.AuthApiKey(api_key=settings.WEAVIATE_API_KEY),
        additional_headers={
            "X-OpenAI-Api-Key": settings.OPENAI_API_KEY
        }
    )

@lru_cache
def convert_user_id(user_id: str):
    if "-" in user_id:
        return user_id.replace("-", "_")
    elif "_" in user_id:
        return user_id.replace("_", "-")
    else:
        return user_id

def get_supabase():
    return create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)
