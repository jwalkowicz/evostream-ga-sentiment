from datetime import datetime
from river import stream, metrics
from src.core.logger import logger

class StreamClusterer:
    """Wrapper for online clustering model and metrics."""
    
    def __init__(self, model):
        self.model = model
        self.silhouette_score = metrics.Silhouette()

    def update(self, embeddings: list):
        """Learns from a batch of embeddings and updates metrics."""
        for x, _ in stream.iter_array(embeddings):
            self.model.learn_one(x)
            y_pred = self.model.predict_one(x)
            
            # Update silhouette score only if there are at least 2 clusters
            if hasattr(self.model, "centers") and len(self.model.centers) >= 2:
                self.silhouette_score.update(x, y_pred, self.model.centers)

    def get_metrics(self) -> dict:
        """Returns current performance snapshot."""
        score = self.silhouette_score.get()
        return {
            "timestamp": datetime.now().isoformat(),
            "n_clusters": getattr(self.model, 'n_clusters', 0),
            "silhouette": score if score != 0 else None
        }
