"""
Script per aggiornamento layer GeoPackage - QGIS Processing Toolbox
Versione pragmatica che usa solo l'API di QGIS (no SQL diretto)
"""

from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (QgsProcessing,
                       QgsFeatureSink,
                       QgsProcessingException,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterVectorLayer,
                       QgsProcessingParameterFeatureSink,
                       QgsProcessingParameterString,
                       QgsProcessingParameterBoolean,
                       QgsProcessingParameterFile,
                       QgsProcessingOutputString,
                       QgsVectorLayer,
                       QgsFeature,
                       QgsField,
                       QgsFields,
                       QgsWkbTypes,
                       QgsCoordinateReferenceSystem,
                       QgsFeatureRequest,
                       QgsVectorFileWriter,
                       QgsProject)
from qgis import processing
import os
import shutil
from datetime import datetime


class GeoPackageUpdaterAlgorithm(QgsProcessingAlgorithm):
    """
    Algoritmo per aggiornare un layer GeoPackage condiviso con le modifiche
    provenienti da un layer GeoPackage utente - Versione API Only
    """

    # Costanti per i parametri
    SHARED_LAYER = 'SHARED_LAYER'
    USER_LAYER = 'USER_LAYER'
    KEY_FIELD = 'KEY_FIELD'
    PREVIEW_ONLY = 'PREVIEW_ONLY'
    OUTPUT_REPORT = 'OUTPUT_REPORT'

    def tr(self, string):
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return GeoPackageUpdaterAlgorithm()

    def name(self):
        return 'geopackage_updater_api_only'

    def displayName(self):
        return self.tr('Aggiorna GeoPackage')

    def group(self):
        return self.tr('Ufficio Patrimonio')

    def groupId(self):
        return 'patrimonio'

    def shortHelpString(self):
        return self.tr("""
Versione semplificata che usa solo l'API di QGIS, no SQL diretto.

Parametri:
- Layer Condiviso: Il layer del GeoPackage condiviso da aggiornare
- Layer Utente: Il layer dell'utente contenente le modifiche
- Campo Chiave: Il campo utilizzato come chiave primaria (default: fuuid)
- Solo Anteprima: Se selezionato, mostra solo le differenze senza aggiornare

Crea automaticamente un backup prima dell'aggiornamento.
        """)

    def initAlgorithm(self, config=None):
        # Layer condiviso (quello da aggiornare)
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.SHARED_LAYER,
                self.tr('Layer GeoPackage Condiviso'),
                [QgsProcessing.TypeVectorAnyGeometry]
            )
        )

        # Layer utente (quello con le modifiche)
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.USER_LAYER,
                self.tr('Layer Utente (con modifiche)'),
                [QgsProcessing.TypeVectorAnyGeometry]
            )
        )

        # Campo chiave per il confronto
        self.addParameter(
            QgsProcessingParameterString(
                self.KEY_FIELD,
                self.tr('Campo Chiave Primaria'),
                defaultValue='fuuid'
            )
        )

        # Modalità solo anteprima
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.PREVIEW_ONLY,
                self.tr('Solo Anteprima (non aggiornare)'),
                defaultValue=True
            )
        )

        # Output del rapporto
        self.addOutput(
            QgsProcessingOutputString(
                self.OUTPUT_REPORT,
                self.tr('Rapporto Aggiornamento')
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        """
        Algoritmo principale di elaborazione
        """
        # Ottieni i parametri
        shared_layer = self.parameterAsVectorLayer(parameters, self.SHARED_LAYER, context)
        user_layer = self.parameterAsVectorLayer(parameters, self.USER_LAYER, context)
        key_field = self.parameterAsString(parameters, self.KEY_FIELD, context)
        preview_only = self.parameterAsBool(parameters, self.PREVIEW_ONLY, context)

        if not shared_layer or not user_layer:
            raise QgsProcessingException(self.tr('Layer non validi'))

        # Verifica che entrambi i layer abbiano il campo chiave
        if key_field not in [field.name() for field in shared_layer.fields()]:
            raise QgsProcessingException(
                self.tr(f'Campo chiave "{key_field}" non trovato nel layer condiviso')
            )

        if key_field not in [field.name() for field in user_layer.fields()]:
            raise QgsProcessingException(
                self.tr(f'Campo chiave "{key_field}" non trovato nel layer utente')
            )

        feedback.pushInfo(self.tr('Inizio analisi delle differenze...'))

        # Analizza le differenze
        new_features, modified_features, report = self.analyze_differences(
            shared_layer, user_layer, key_field, feedback
        )

        feedback.pushInfo(report)

        # Se non è solo anteprima, procedi con l'aggiornamento
        if not preview_only and (new_features or modified_features):
            feedback.pushInfo(self.tr('Procedendo con l\'aggiornamento...'))
            
            update_report = self.update_with_qgis_api_only(
                shared_layer, new_features, modified_features, key_field, feedback
            )
            
            report += "\n" + update_report

        return {self.OUTPUT_REPORT: report}

    def analyze_differences(self, shared_layer, user_layer, key_field, feedback):
        """
        Analizza le differenze tra i due layer (questa parte funziona già)
        """
        feedback.setProgress(10)
        
        # Raccogli tutti i record dei layer
        shared_features = {}
        user_features = {}

        # Carica features del layer condiviso
        for feature in shared_layer.getFeatures():
            key_value = str(feature[key_field])
            shared_features[key_value] = feature

        feedback.setProgress(30)

        # Carica features del layer utente
        for feature in user_layer.getFeatures():
            key_value = str(feature[key_field])
            user_features[key_value] = feature

        feedback.setProgress(50)

        # Trova nuovi record (presenti in user ma non in shared)
        new_keys = set(user_features.keys()) - set(shared_features.keys())
        new_features = [user_features[key] for key in new_keys]

        # Trova record modificati
        modified_features = []
        common_keys = set(shared_features.keys()) & set(user_features.keys())

        feedback.setProgress(70)

        for key in common_keys:
            shared_feat = shared_features[key]
            user_feat = user_features[key]
            
            differences = self.compare_features(shared_feat, user_feat, key_field)
            if differences:
                modified_features.append({
                    'feature': user_feat,
                    'key': key,
                    'differences': differences
                })

        feedback.setProgress(90)

        # Genera il rapporto
        report = self.generate_report(new_features, modified_features, key_field)

        feedback.setProgress(100)

        return new_features, modified_features, report

    def compare_features(self, shared_feat, user_feat, key_field):
        """
        Confronta due feature e restituisce le differenze
        """
        differences = []
        
        # Confronta tutti i campi (escludi il campo chiave)
        for field in shared_feat.fields():
            field_name = field.name()
            if field_name == key_field:
                continue
                
            # Controlla se il campo esiste in entrambi i layer
            if field_name not in user_feat.fields().names():
                continue
                
            shared_value = shared_feat[field_name]
            user_value = user_feat[field_name]
            
            # Normalizza i valori NULL/vuoti per il confronto
            shared_normalized = self.normalize_value(shared_value)
            user_normalized = self.normalize_value(user_value)
            
            # Confronta solo se effettivamente diversi
            if shared_normalized != user_normalized:
                differences.append(f"{field_name}: '{shared_value}' → '{user_value}'")

        # Confronta la geometria solo se entrambe esistono e sono valide
        shared_geom = shared_feat.geometry()
        user_geom = user_feat.geometry()
        
        if (shared_geom and user_geom and 
            shared_geom.isGeosValid() and user_geom.isGeosValid() and
            not shared_geom.equals(user_geom)):
            differences.append("geometry: modificata")

        return differences
    
    def normalize_value(self, value):
        """
        Normalizza i valori per il confronto gestendo NULL, None, stringhe vuote, ecc.
        """
        # Se è None o NULL
        if value is None:
            return None
            
        # Se è una stringa
        if isinstance(value, str):
            # Stringa vuota o "NULL" -> None
            if value == '' or value.upper() == 'NULL':
                return None
            # Rimuovi spazi bianchi
            return value.strip()
            
        # Se è un numero (int, float)
        if isinstance(value, (int, float)):
            return value
            
        # Per altri tipi, converti a stringa e normalizza
        str_value = str(value).strip()
        if str_value == '' or str_value.upper() == 'NULL':
            return None
            
        return str_value

    def generate_report(self, new_features, modified_features, key_field):
        """
        Genera il rapporto delle differenze
        """
        report = []
        report.append("=== RAPPORTO AGGIORNAMENTO GEOPACKAGE ===")
        report.append(f"Data/Ora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Campo chiave utilizzato: {key_field}")
        report.append("")

        # Nuovi record
        if new_features:
            report.append(f"🆕 NUOVI RECORD DA INSERIRE: {len(new_features)}")
            for feature in new_features[:10]:  # Mostra solo i primi 10
                key_value = feature[key_field]
                report.append(f"  - {key_field}: {key_value}")
            if len(new_features) > 10:
                report.append(f"  ... e altri {len(new_features) - 10} record")
            report.append("")

        # Record modificati
        if modified_features:
            report.append(f"✏️ RECORD MODIFICATI: {len(modified_features)}")
            for mod_feat in modified_features[:10]:  # Mostra solo i primi 10
                report.append(f"  - {key_field}: {mod_feat['key']}")
                for diff in mod_feat['differences'][:5]:  # Mostra solo le prime 5 differenze
                    report.append(f"    {diff}")
                if len(mod_feat['differences']) > 5:
                    report.append(f"    ... e altre {len(mod_feat['differences']) - 5} differenze")
                report.append("")
            if len(modified_features) > 10:
                report.append(f"  ... e altri {len(modified_features) - 10} record modificati")

        if not new_features and not modified_features:
            report.append("✅ NESSUNA DIFFERENZA TROVATA")
            report.append("I layer sono già sincronizzati.")

        return "\n".join(report)

    def update_with_qgis_api_only(self, shared_layer, new_features, modified_features, key_field, feedback):
        """
        Aggiorna usando SOLO l'API di QGIS - niente SQL
        """
        try:
            # Crea backup del file
            source = shared_layer.source()
            if '|' in source:
                file_path = source.split('|')[0]
            else:
                file_path = source

            if os.path.exists(file_path) and file_path.endswith('.gpkg'):
                backup_path = f"{file_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                shutil.copy2(file_path, backup_path)
                feedback.pushInfo(f"Backup creato: {backup_path}")

            # Assicurati che non sia in editing
            if shared_layer.isEditable():
                shared_layer.commitChanges()

            # Inizia editing
            feedback.pushInfo("Iniziando modalità editing...")
            if not shared_layer.startEditing():
                return "❌ Impossibile iniziare editing del layer"

            report_lines = []

            try:
                # Aggiungi nuovi record
                if new_features:
                    feedback.pushInfo(f"Aggiunta di {len(new_features)} nuovi record...")
                    
                    for feature in new_features:
                        # Crea nuova feature compatibile
                        new_feat = QgsFeature(shared_layer.fields())
                        
                        # Copia solo i campi che esistono in entrambi i layer
                        for field in shared_layer.fields():
                            field_name = field.name()
                            if field_name in feature.fields().names():
                                new_feat[field_name] = feature[field_name]
                        
                        # Copia geometria
                        if feature.geometry():
                            new_feat.setGeometry(feature.geometry())
                        
                        # Aggiungi al layer
                        if not shared_layer.addFeature(new_feat):
                            feedback.pushInfo(f"⚠️ Problemi aggiunta record {feature[key_field]}")

                    report_lines.append(f"✅ Aggiunti {len(new_features)} nuovi record")

                # Aggiorna record esistenti
                if modified_features:
                    feedback.pushInfo(f"Aggiornamento di {len(modified_features)} record...")
                    
                    for mod_feat_data in modified_features:
                        key_value = mod_feat_data['key']
                        new_feature = mod_feat_data['feature']
                        
                        # Trova il record esistente
                        request = QgsFeatureRequest().setFilterExpression(f'"{key_field}" = \'{key_value}\'')
                        existing_features = list(shared_layer.getFeatures(request))
                        
                        if existing_features:
                            existing_feat = existing_features[0]
                            fid = existing_feat.id()
                            
                            # Aggiorna campo per campo
                            for i, field in enumerate(shared_layer.fields()):
                                field_name = field.name()
                                if field_name == key_field:
                                    continue  # Non modificare la chiave
                                
                                if field_name in new_feature.fields().names():
                                    new_value = new_feature[field_name]
                                    shared_layer.changeAttributeValue(fid, i, new_value)
                            
                            # Aggiorna geometria
                            if new_feature.geometry():
                                shared_layer.changeGeometry(fid, new_feature.geometry())

                    report_lines.append(f"✅ Aggiornati {len(modified_features)} record")

                # Commit delle modifiche
                feedback.pushInfo("Salvando modifiche...")
                if shared_layer.commitChanges():
                    report_lines.append("🎉 AGGIORNAMENTO COMPLETATO CON SUCCESSO!")
                    
                    # Ricarica il layer
                    shared_layer.reload()
                    
                    return "\n".join(report_lines)
                else:
                    # Errori durante il commit
                    errors = shared_layer.commitErrors()
                    error_msg = f"❌ Errori durante il salvataggio: {'; '.join(errors)}"
                    feedback.pushInfo(error_msg)
                    shared_layer.rollBack()
                    return error_msg

            except Exception as e:
                # Rollback in caso di errore
                shared_layer.rollBack()
                return f"❌ Errore durante l'aggiornamento: {str(e)}"

        except Exception as e:
            return f"❌ Errore generale: {str(e)}"


def classFactory(iface):
    """
    Factory function per il plugin (se necessario)
    """
    return GeoPackageUpdaterAlgorithm()