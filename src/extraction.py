# Module d'extraction des données de consommation électrique
# Source : ODRE - Open Data Réseaux Énergies
# imports
import requests
import os
from pathlib import Path
from datetime import datetime
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# URL du dataset ODRE consommation électrique régionale
BASE_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/consommation-quotidienne-brute-regionale/exports/csv"

# Paramètres de l'API
DEFAULT_PARAMS = {
    'limit': -1,  # -1 = tous les enregistrements
    'timezone': 'Europe/Paris',
    'use_labels': 'true',
    'delimiter': ';'
}

# Chemins des dossiers
DATA_RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

def download_energy_data(output_filename=None, params=None):
    """
    Telecharge les donnees de consommation electrique depuis l'API ODRE
    
    Args:
        output_filename (str, optional): Nom du fichier de sortie.
                                         Par defaut: energy_data_YYYYMMDD_HHMMSS.csv
        params (dict, optional): Parametres supplementaires pour l'API.
                                Par defaut: utilise DEFAULT_PARAMS
    
    Returns:
        Path: Chemin vers le fichier telecharge
    
    Raises:
        requests.RequestException: En cas d'erreur HTTP
        IOError: En cas d'erreur d'ecriture fichier
    """
    
    # Creer le dossier data/raw/ s'il n'existe pas
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    
    # Generer le nom de fichier avec timestamp si non fourni
    if output_filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"energy_data_{timestamp}.csv"
    
    output_path = DATA_RAW_DIR / output_filename
    
    # Fusionner les parametres par defaut avec ceux fournis
    request_params = DEFAULT_PARAMS.copy()
    if params:
        request_params.update(params)
    
    logger.info(f"Telechargement des donnees depuis {BASE_URL}")
    logger.info(f"Parametres: {request_params}")

# Bloc try/except pour gerer les erreurs HTTP
    try:
        # Requete HTTP GET avec timeout
        response = requests.get(
            BASE_URL,
            params=request_params,
            timeout=60,
            stream=True
        )
        
        # Verifier le code de statut HTTP
        response.raise_for_status()
        
        # Recuperer la taille totale du fichier
        total_size = int(response.headers.get('content-length', 0))
        logger.info(f"Taille du fichier: {total_size / 1024 / 1024:.2f} MB")
        
        # Ecrire le fichier par chunks pour economiser la memoire
        chunk_size = 8192  # 8 KB par chunk
        downloaded_size = 0
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    
                    # Afficher la progression tous les 10%
                    if total_size > 0:
                        progress = (downloaded_size / total_size) * 100
                        if int(progress) % 10 == 0:
                            logger.info(f"Progression: {progress:.1f}%")
        
        logger.info(f"Telechargement termine: {output_path}")
        return output_path

    
    except requests.exceptions.Timeout:
        logger.error("Timeout: le serveur met trop de temps a repondre")
        raise
    
    except requests.exceptions.HTTPError as e:
        logger.error(f"Erreur HTTP: {e}")
        logger.error(f"Code de statut: {response.status_code}")
        raise
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors du telechargement: {e}")
        raise
    
    except IOError as e:
        logger.error(f"Erreur lors de l'ecriture du fichier: {e}")
        raise


if __name__ == "__main__":
    # Code execute uniquement si le fichier est lance directement
    # (pas quand il est importe comme module)
        
    try:
        logger.info("=== Debut extraction donnees energie ===")
            
        # Telecharger les donnees
        output_file = download_energy_data()
            
        logger.info(f"Fichier sauvegarde: {output_file}")
        logger.info("=== Extraction terminee avec succes ===")
            
    except Exception as e:
        logger.error(f"Echec de l'extraction: {e}")
        exit(1)
