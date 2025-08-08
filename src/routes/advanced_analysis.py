from flask import Blueprint, request, jsonify
import logging
from services.social_news_monitor import SocialNewsMonitor
from services.competitor_content_collector import CompetitorContentCollector
from services.media_trend_analyzer import MediaTrendAnalyzer
from services.report_automation_manager import ReportAutomationManager

advanced_analysis_bp = Blueprint("advanced_analysis", __name__)
logger = logging.getLogger(__name__)

# Inicializa os managers
try:
    social_news_monitor = SocialNewsMonitor()
    competitor_content_collector = CompetitorContentCollector()
    media_trend_analyzer = MediaTrendAnalyzer()
    report_automation_manager = ReportAutomationManager()
except Exception as e:
    logger.error(f"Erro ao inicializar managers de análise avançada: {e}")
    social_news_monitor = None
    competitor_content_collector = None
    media_trend_analyzer = None
    report_automation_manager = None

# --- Rotas para Automação de Relatórios Personalizados (1.2) ---
@advanced_analysis_bp.route("/reports/generate", methods=["POST"])
def generate_custom_report():
    if not report_automation_manager:
        return jsonify({"error": "Serviço de Automação de Relatórios não configurado."}), 500
    data = request.json
    report_params = data.get("report_params")

    if not report_params:
        return jsonify({"error": "Parâmetros do relatório são obrigatórios."}), 400

    result = report_automation_manager.generate_report(report_params)
    return jsonify(result)

# --- Rotas para Monitoramento de Mídias Sociais e Notícias em Tempo Real (2.1) ---
@advanced_analysis_bp.route("/social_news/monitor", methods=["POST"])
def monitor_social_news():
    if not social_news_monitor:
        return jsonify({"error": "Serviço de Monitoramento Social/Notícias não configurado."}), 500
    data = request.json
    keywords = data.get("keywords")
    search_sources = data.get("search_sources")
    time_range_days = data.get("time_range_days", 1)

    if not keywords or not isinstance(keywords, list):
        return jsonify({"error": "Lista de palavras-chave é obrigatória."}), 400

    mentions = social_news_monitor.monitor_keywords(keywords, search_sources, time_range_days)
    return jsonify({"status": "success", "mentions_count": len(mentions), "mentions": mentions})

@advanced_analysis_bp.route("/social_news/summary", methods=["GET"])
def get_social_news_summary():
    if not social_news_monitor:
        return jsonify({"error": "Serviço de Monitoramento Social/Notícias não configurado."}), 500
    keywords = request.args.getlist("keywords")
    summary = social_news_monitor.get_mentions_summary(keywords if keywords else None)
    return jsonify(summary)

# --- Rotas para Análise de Conteúdo de Concorrentes (2.2) ---
@advanced_analysis_bp.route("/competitors/add", methods=["POST"])
def add_competitor_config():
    if not competitor_content_collector:
        return jsonify({"error": "Serviço de Coleta de Conteúdo de Concorrentes não configurado."}), 500
    data = request.json
    name = data.get("name")
    base_urls = data.get("base_urls")

    if not name or not base_urls or not isinstance(base_urls, list):
        return jsonify({"error": "Nome e URLs base do concorrente são obrigatórios."}), 400

    competitor_content_collector.add_competitor(name, base_urls)
    return jsonify({"status": "success", "message": f"Concorrente {name} adicionado/atualizado."})

@advanced_analysis_bp.route("/competitors/collect_analyze", methods=["POST"])
def collect_analyze_competitor_content():
    if not competitor_content_collector:
        return jsonify({"error": "Serviço de Coleta de Conteúdo de Concorrentes não configurado."}), 500
    data = request.json
    competitor_name = data.get("competitor_name")

    if not competitor_name:
        return jsonify({"error": "Nome do concorrente é obrigatório."}), 400

    new_content = competitor_content_collector.collect_and_analyze_content(competitor_name)
    return jsonify({"status": "success", "new_content_count": len(new_content), "new_content": new_content})

@advanced_analysis_bp.route("/competitors/summary", methods=["GET"])
def get_competitor_content_summary():
    if not competitor_content_collector:
        return jsonify({"error": "Serviço de Coleta de Conteúdo de Concorrentes não configurado."}), 500
    competitor_name = request.args.get("competitor_name")
    summary = competitor_content_collector.get_competitor_content_summary(competitor_name)
    return jsonify(summary)

# --- Rotas para Análise de Tendências de Vídeo e Áudio (2.3) ---
@advanced_analysis_bp.route("/media_trends/analyze_videos", methods=["POST"])
def analyze_video_trends():
    if not media_trend_analyzer:
        return jsonify({"error": "Serviço de Análise de Tendências de Mídia não configurado."}), 500
    data = request.json
    video_urls = data.get("video_urls")

    if not video_urls or not isinstance(video_urls, list):
        return jsonify({"error": "Lista de URLs de vídeo é obrigatória."}), 400

    results = media_trend_analyzer.analyze_video_trends(video_urls)
    return jsonify({"status": "success", "analyzed_count": len(results), "results": results})

@advanced_analysis_bp.route("/media_trends/summary", methods=["GET"])
def get_media_trends_summary():
    if not media_trend_analyzer:
        return jsonify({"error": "Serviço de Análise de Tendências de Mídia não configurado."}), 500
    summary = media_trend_analyzer.get_analyzed_media_summary()
    return jsonify(summary)


