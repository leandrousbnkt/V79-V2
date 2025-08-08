import os
import requests
import logging

logger = logging.getLogger(__name__)

class MCPSupadataManager:
    def __init__(self):
        self.base_url = os.getenv("MCP_SUPADATA_URL")
        if not self.base_url:
            logger.error("MCP_SUPADATA_URL não configurado nas variáveis de ambiente.")
            raise ValueError("MCP_SUPADATA_URL não configurado.")

    def extract_from_url(self, url: str) -> dict:
        """Extrai dados estruturados de uma URL."""
        endpoint = f"{self.base_url}/extract_url"
        payload = {"url": url}
        try:
            response = requests.post(endpoint, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao extrair dados da URL {url}: {e}")
            return {"error": str(e)}

    def extract_from_video(self, video_url: str) -> dict:
        """Extrai dados estruturados de um vídeo (ex: transcrição)."""
        endpoint = f"{self.base_url}/extract_video"
        payload = {"video_url": video_url}
        try:
            response = requests.post(endpoint, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao extrair dados do vídeo {video_url}: {e}")
            return {"error": str(e)}

# Exemplo de uso (apenas para demonstração, remover em produção)
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    manager = MCPSupadataManager()

    # Exemplo de extração de URL
    print("Extraindo dados de uma URL...")
    url_to_extract = "https://www.smithery.ai/"
    result_url = manager.extract_from_url(url_to_extract)
    print(result_url)

    # Exemplo de extração de vídeo (substitua por uma URL de vídeo real se for testar)
    print("Extraindo dados de um vídeo...")
    video_to_extract = "https://www.youtube.com/watch?v=dQw4w9WgXcQ" # Exemplo, substitua por um vídeo real
    result_video = manager.extract_from_video(video_to_extract)
    print(result_video)


