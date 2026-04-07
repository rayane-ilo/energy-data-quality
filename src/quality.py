# Module de validation qualite des donnees avec Great Expectations

import pandas as pd
from pathlib import Path
import logging
import great_expectations as gx
from great_expectations.core import ExpectationConfiguration

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chemins
DATA_PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"


def create_expectation_suite(df):
    # Cree une suite d'expectations pour valider les donnees
    # Args:
    #   df (pd.DataFrame): Donnees a valider
    # Returns:
    #   list: Liste des resultats de validation
    
    logger.info("Creation de la suite d'expectations")
    
    # Contexte Great Expectations
    context = gx.get_context()
    
    # Creer un Batch depuis le DataFrame
    datasource = context.sources.add_pandas(name="pandas_datasource")
    data_asset = datasource.add_dataframe_asset(name="energy_data")
    batch_request = data_asset.build_batch_request(dataframe=df)
    
    # Recuperer le batch
    validator = context.get_validator(batch_request=batch_request)
    
    logger.info("Ajout des expectations")
    
    # === EXPECTATIONS SUR LES COLONNES CLES ===
    
    # 1. Colonnes obligatoires (pas de valeurs nulles)
    colonnes_obligatoires = [
        'Date',
        'heure',
        'code_region',
        'region',
        'consommation_elec_mw'
    ]
    
    for col in colonnes_obligatoires:
        validator.expect_column_values_to_not_be_null(column=col)
    
    # 2. Consommation electrique: valeurs positives
    validator.expect_column_values_to_be_between(
        column='consommation_elec_mw',
        min_value=0,
        max_value=None
    )
    
    # 3. Consommation electrique: valeurs non extremes (outliers)
    # Basé sur l'analyse exploratoire: max ~15000 MW
    validator.expect_column_values_to_be_between(
        column='consommation_elec_mw',
        min_value=0,
        max_value=20000,
        mostly=0.999  # 99.9% des valeurs doivent respecter cette regle
    )
    
    # 4. Code region: valeurs autorisees (13 regions francaises)
    regions_valides = [11, 24, 27, 28, 32, 44, 52, 53, 75, 76, 84, 93, 94]
    validator.expect_column_values_to_be_in_set(
        column='code_region',
        value_set=regions_valides
    )
    
    # 5. Statut RTE: valeurs autorisees
    statuts_valides = ['Définitif', 'Consolidé']
    validator.expect_column_values_to_be_in_set(
        column='statut_rte',
        value_set=statuts_valides
    )
    
    # 6. Type de jour: valeurs autorisees
    validator.expect_column_values_to_be_in_set(
        column='type_jour',
        value_set=['semaine', 'weekend']
    )
    
    # 7. Tranche horaire: valeurs autorisees
    validator.expect_column_values_to_be_in_set(
        column='tranche_horaire',
        value_set=['matin', 'apres-midi', 'soir', 'nuit']
    )
    
    # 8. Annee: plage coherente
    validator.expect_column_values_to_be_between(
        column='annee',
        min_value=2013,
        max_value=2026
    )
    
    # 9. Mois: plage valide
    validator.expect_column_values_to_be_between(
        column='mois',
        min_value=1,
        max_value=12
    )
    
    # 10. Jour: plage valide
    validator.expect_column_values_to_be_between(
        column='jour',
        min_value=1,
        max_value=31
    )
    
    # 11. Heure numerique: plage valide
    validator.expect_column_values_to_be_between(
        column='heure_num',
        min_value=0,
        max_value=23
    )
    
    # 12. Minute numerique: valeurs autorisees (0 ou 30)
    validator.expect_column_values_to_be_in_set(
        column='minute_num',
        value_set=[0, 30]
    )
    
    # 13. Unicite de la combinaison Date + Heure + Region
    # (chaque observation doit etre unique)
    validator.expect_compound_columns_to_be_unique(
        column_list=['Date', 'heure', 'region']
    )
    
    logger.info("Suite d'expectations creee avec succes")
    
    return validator


def validate_data(df):
    # Valide les donnees avec Great Expectations
    # Args:
    #   df (pd.DataFrame): Donnees a valider
    # Returns:
    #   dict: Resultats de la validation
    
    logger.info("=== DEBUT DE LA VALIDATION QUALITE ===")
    
    # Creer et executer la suite d'expectations
    validator = create_expectation_suite(df)
    
    # Executer la validation
    logger.info("Execution de la validation")
    results = validator.validate()
    
    # Analyser les resultats
    success_count = results.statistics['successful_expectations']
    total_count = results.statistics['evaluated_expectations']
    success_rate = (success_count / total_count) * 100
    
    logger.info(f"Validation terminee:")
    logger.info(f"  - Expectations reussies: {success_count}/{total_count}")
    logger.info(f"  - Taux de reussite: {success_rate:.2f}%")
    
    # Afficher les echecs
    if not results.success:
        logger.warning("Des expectations ont echoue:")
        for result in results.results:
            if not result.success:
                expectation = result.expectation_config.expectation_type
                column = result.expectation_config.kwargs.get('column', 'N/A')
                logger.warning(f"  - {expectation} sur colonne '{column}'")
    
    logger.info("=== FIN DE LA VALIDATION QUALITE ===")
    
    return {
        'success': results.success,
        'success_rate': success_rate,
        'total_expectations': total_count,
        'successful_expectations': success_count,
        'failed_expectations': total_count - success_count,
        'results': results
    }


def generate_quality_report(validation_results, output_path=None):
    # Genere un rapport de qualite au format texte
    # Args:
    #   validation_results (dict): Resultats de la validation
    #   output_path (Path, optional): Chemin du fichier de rapport
    
    logger.info("Generation du rapport de qualite")
    
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("RAPPORT DE QUALITE DES DONNEES")
    report_lines.append("=" * 80)
    report_lines.append("")
    report_lines.append(f"Statut global: {'✓ SUCCES' if validation_results['success'] else '✗ ECHEC'}")
    report_lines.append(f"Taux de reussite: {validation_results['success_rate']:.2f}%")
    report_lines.append(f"Expectations reussies: {validation_results['successful_expectations']}/{validation_results['total_expectations']}")
    report_lines.append("")
    
    # Details des echecs
    if validation_results['failed_expectations'] > 0:
        report_lines.append("EXPECTATIONS ECHOUEES:")
        report_lines.append("-" * 80)
        
        for result in validation_results['results'].results:
            if not result.success:
                expectation = result.expectation_config.expectation_type
                column = result.expectation_config.kwargs.get('column', 'N/A')
                report_lines.append(f"  - {expectation}")
                report_lines.append(f"    Colonne: {column}")
                report_lines.append(f"    Details: {result.result}")
                report_lines.append("")
    
    report_lines.append("=" * 80)
    
    report_text = "\n".join(report_lines)
    
    # Afficher le rapport
    print(report_text)
    
    # Sauvegarder si chemin fourni
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
        logger.info(f"Rapport sauvegarde: {output_path}")
    
    return report_text


if __name__ == "__main__":
    # Execution de la validation sur les donnees transformees
    
    try:
        # Charger les donnees transformees
        data_file = DATA_PROCESSED_DIR / "energy_clean.csv"
        
        if not data_file.exists():
            logger.error(f"Fichier introuvable: {data_file}")
            logger.error("Executez d'abord: python src/transform.py")
            exit(1)
        
        logger.info(f"Chargement des donnees depuis: {data_file}")
        df = pd.read_csv(data_file, sep=';', parse_dates=['Date'])
        logger.info(f"Donnees chargees: {len(df):,} lignes x {len(df.columns)} colonnes")
        
        # Validation
        validation_results = validate_data(df)
        
        # Generer le rapport
        report_path = DATA_PROCESSED_DIR / "quality_report.txt"
        generate_quality_report(validation_results, report_path)
        
        # Code de sortie selon le resultat
        exit(0 if validation_results['success'] else 1)
        
    except Exception as e:
        logger.error(f"Erreur lors de la validation: {e}")
        exit(1)

