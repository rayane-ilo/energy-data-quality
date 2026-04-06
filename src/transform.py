# Module de transformation et nettoyage des donnees
# Traite les donnees brutes extraites de l'API ODRE
# imports
import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chemins
DATA_RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
DATA_PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"


def load_raw_data(filename=None):
    # Charge les donnees brutes depuis data/raw/
    # Args:
    #   filename (str, optional): Nom du fichier. Si None, charge le plus recent.
    # Returns:
    #   pd.DataFrame: Donnees brutes chargees
    
    if filename is None:
        # Trouver le fichier le plus recent
        csv_files = list(DATA_RAW_DIR.glob("energy_data_*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"Aucun fichier trouve dans {DATA_RAW_DIR}")
        
        latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Fichier le plus recent trouve: {latest_file.name}")
    else:
        latest_file = DATA_RAW_DIR / filename
        if not latest_file.exists():
            raise FileNotFoundError(f"Fichier introuvable: {latest_file}")
    
    logger.info(f"Chargement des donnees depuis: {latest_file}")
    df = pd.read_csv(latest_file, sep=';', low_memory=False)
    logger.info(f"Donnees chargees: {len(df):,} lignes x {len(df.columns)} colonnes")
    
    return df


def clean_data(df):
    # Nettoie les donnees brutes
    # Etapes:
    # - Conversion des types de donnees
    # - Correction des valeurs aberrantes
    # - Suppression des colonnes inutiles
    # Args:
    #   df (pd.DataFrame): Donnees brutes
    # Returns:
    #   pd.DataFrame: Donnees nettoyees
    
    logger.info("Debut du nettoyage des donnees")
    
    df_clean = df.copy()
    
    # Conversion de la colonne Date en datetime
    logger.info("Conversion des dates")
    df_clean['Date'] = pd.to_datetime(df_clean['Date'], format='%Y-%m-%d', errors='coerce')
    
    # Renommer les colonnes pour simplifier
    logger.info("Renommage des colonnes")
    df_clean = df_clean.rename(columns={
        'Code INSEE région': 'code_region',
        'Région': 'region',
        'Consommation brute électricité (MW) - RTE': 'consommation_elec_mw',
        'Statut - RTE': 'statut_rte',
        'Consommation brute gaz (MW PCS 0°C) - NaTran': 'consommation_gaz_natran_mw',
        'Statut - NaTran': 'statut_natran',
        'Consommation brute gaz (MW PCS 0°C) - Teréga': 'consommation_gaz_terega_mw',
        'Statut - Teréga': 'statut_terega',
        'Consommation brute gaz totale (MW PCS 0°C)': 'consommation_gaz_total_mw',
        'Consommation brute totale (MW)': 'consommation_totale_mw',
        'Date - Heure': 'date_heure',
        'Heure': 'heure'
    })
    
    # Correction de la valeur negative identifiee dans l'analyse
    # Date: 2025-11-13, Heure: 11:30, Region: Auvergne-Rhone-Alpes, Valeur: -3239
    mask_negative = (
        (df_clean['Date'] == '2025-11-13') & 
        (df_clean['heure'] == '11:30') & 
        (df_clean['region'] == 'Auvergne-Rhône-Alpes')
    )
    
    if mask_negative.any():
        logger.warning(f"Correction de la valeur negative: {df_clean.loc[mask_negative, 'consommation_elec_mw'].values[0]} MW")
        df_clean.loc[mask_negative, 'consommation_elec_mw'] = abs(df_clean.loc[mask_negative, 'consommation_elec_mw'])
        logger.info(f"Nouvelle valeur: {df_clean.loc[mask_negative, 'consommation_elec_mw'].values[0]} MW")
    
    # Suppression de la colonne flag_ignore (non documentee)
    if 'flag_ignore' in df_clean.columns:
        df_clean = df_clean.drop(columns=['flag_ignore'])
        logger.info("Colonne 'flag_ignore' supprimee")
    
    # Filtrer les donnees validees uniquement (Definitif ou Consolide)
    initial_count = len(df_clean)
    df_clean = df_clean[df_clean['statut_rte'].isin(['Définitif', 'Consolidé'])]
    filtered_count = initial_count - len(df_clean)
    logger.info(f"Filtrage statut RTE: {filtered_count:,} lignes supprimees ({filtered_count/initial_count*100:.2f}%)")
    
    logger.info(f"Nettoyage termine: {len(df_clean):,} lignes conservees")
    return df_clean


def enrich_data(df):
    # Enrichit les donnees avec des colonnes derivees
    # Ajoute:
    # - Annee, mois, jour, jour_semaine
    # - Trimestre, semestre
    # - Heure numerique (pour analyses)
    # - Type de jour (semaine/weekend)
    # Args:
    #   df (pd.DataFrame): Donnees nettoyees
    # Returns:
    #   pd.DataFrame: Donnees enrichies
    
    logger.info("Enrichissement des donnees avec colonnes derivees")
    
    df_enriched = df.copy()
    
    # Extraction des composantes temporelles
    df_enriched['annee'] = df_enriched['Date'].dt.year
    df_enriched['mois'] = df_enriched['Date'].dt.month
    df_enriched['jour'] = df_enriched['Date'].dt.day
    df_enriched['jour_semaine'] = df_enriched['Date'].dt.dayofweek  # 0=Lundi, 6=Dimanche
    df_enriched['nom_jour'] = df_enriched['Date'].dt.day_name()
    df_enriched['trimestre'] = df_enriched['Date'].dt.quarter
    df_enriched['semestre'] = df_enriched['Date'].dt.month.apply(lambda x: 1 if x <= 6 else 2)
    
    # Type de jour (semaine/weekend)
    df_enriched['type_jour'] = df_enriched['jour_semaine'].apply(
        lambda x: 'weekend' if x >= 5 else 'semaine'
    )
    
    # Conversion heure en numerique (pour analyses)
    df_enriched['heure_num'] = df_enriched['heure'].str[:2].astype(int)
    df_enriched['minute_num'] = df_enriched['heure'].str[3:5].astype(int)
    
    # Tranche horaire (matin, apres-midi, soir, nuit)
    def get_tranche_horaire(heure):
        if 6 <= heure < 12:
            return 'matin'
        elif 12 <= heure < 18:
            return 'apres-midi'
        elif 18 <= heure < 22:
            return 'soir'
        else:
            return 'nuit'
    
    df_enriched['tranche_horaire'] = df_enriched['heure_num'].apply(get_tranche_horaire)
    
    logger.info(f"Enrichissement termine: {len(df_enriched.columns)} colonnes au total")
    
    return df_enriched


def save_processed_data(df, filename='energy_clean.csv'):
    # Sauvegarde les donnees transformees dans data/processed/
    # Args:
    #   df (pd.DataFrame): Donnees transformees
    #   filename (str): Nom du fichier de sortie
    # Returns:
    #   Path: Chemin vers le fichier sauvegarde
    
    # Creer le dossier processed s'il n'existe pas
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    output_path = DATA_PROCESSED_DIR / filename
    
    logger.info(f"Sauvegarde des donnees transformees: {output_path}")
    df.to_csv(output_path, index=False, sep=';')
    logger.info(f"Fichier sauvegarde: {output_path} ({output_path.stat().st_size / 1024 / 1024:.2f} MB)")
    
    return output_path


def transform_pipeline(input_filename=None, output_filename='energy_clean.csv'):
    # Pipeline complet de transformation
    # Etapes:
    # 1. Chargement des donnees brutes
    # 2. Nettoyage
    # 3. Enrichissement
    # 4. Sauvegarde
    # Args:
    #   input_filename (str, optional): Fichier d'entree. Si None, charge le plus recent.
    #   output_filename (str): Nom du fichier de sortie
    # Returns:
    #   pd.DataFrame: Donnees transformees
    
    logger.info("=== DEBUT DU PIPELINE DE TRANSFORMATION ===")
    
    # Etape 1: Chargement
    df_raw = load_raw_data(input_filename)
    
    # Etape 2: Nettoyage
    df_clean = clean_data(df_raw)
    
    # Etape 3: Enrichissement
    df_enriched = enrich_data(df_clean)
    
    # Etape 4: Sauvegarde
    output_path = save_processed_data(df_enriched, output_filename)
    
    logger.info("=== PIPELINE DE TRANSFORMATION TERMINE ===")
    logger.info(f"Resultat: {len(df_enriched):,} lignes x {len(df_enriched.columns)} colonnes")
    
    return df_enriched


if __name__ == "__main__":
    # Execution du pipeline complet
    try:
        df_transformed = transform_pipeline()
        
        # Afficher un apercu
        print("\n=== APERÇU DES DONNÉES TRANSFORMÉES ===")
        print(df_transformed.head())
        print(f"\nColonnes: {list(df_transformed.columns)}")
        
    except Exception as e:
        logger.error(f"Erreur lors de la transformation: {e}")
        exit(1)

