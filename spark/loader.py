import sys
from pyspark.ml.feature import Word2VecModel, CountVectorizerModel, StringIndexerModel
from pyspark.ml.clustering import LocalLDAModel
from pyspark.ml.regression import RandomForestRegressionModel

class ModelLoader:
    """
    Classe responsable uniquement du chargement des modèles ML depuis le disque.
    """
    def __init__(self, paths: dict):
        self.paths = paths

    def load_all(self):
        print("📥 Loading models from disk...")
        models = {}
        try:
            models['w2v'] = Word2VecModel.load(self.paths['word2vec'])
            models['cv'] = CountVectorizerModel.load(self.paths['cv'])
            models['lda'] = LocalLDAModel.load(self.paths['lda'])
            
            # StringIndexer: Important de gérer les nouvelles valeurs inconnues
            sub_model = StringIndexerModel.load(self.paths['subreddit'])
            models['sub'] = sub_model.setHandleInvalid("keep")
            
            sent_model = StringIndexerModel.load(self.paths['sentiment'])
            models['sent'] = sent_model.setHandleInvalid("keep")
            
            models['rf'] = RandomForestRegressionModel.load(self.paths['rf'])
            
            print("✅ All models loaded successfully.")
            return models
        except Exception as e:
            print(f"❌ Critical Error loading models: {e}")
            sys.exit(1)