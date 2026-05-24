import re
import string

from bs4 import BeautifulSoup
from sklearn.base import BaseEstimator, TransformerMixin


class TextPreprocessor:
    """Handles low-level text cleaning."""

    def clean(self, text: str) -> str:
        """Removes HTML, URLs, punctuation and normalizes text."""
        if not text:
            return ""
        text = text.lower()
        text = BeautifulSoup(text, "html.parser").get_text()
        text = re.sub(r"http\S+|www\S+", "", text)
        text = re.sub(r"\d+", "", text)
        text = text.translate(str.maketrans("", "", string.punctuation))
        text = re.sub(r"\W+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text


class EmbeddingTransformer(BaseEstimator, TransformerMixin):
    """
    Transformer for text encoding and dimensionality reduction.
    """

    def __init__(self, encoder, pca):
        """
        Initializes the transformer with an encoder and a PCA model.

        Args:
            encoder: A model capable of encoding text (e.g., SentenceTransformer).
            pca: A dimensionality reduction model (e.g., IncrementalPCA).
        """
        self.encoder = encoder
        self.pca = pca

    def fit(self, X, y=None):
        return self

    def partial_fit(self, X, y=None):
        embeddings = self.encoder.encode(X, show_progress_bar=False)
        if len(embeddings) >= self.pca.n_components:
            self.pca.partial_fit(embeddings)
        return self

    def transform(self, X):
        embeddings = self.encoder.encode(X, show_progress_bar=False)
        return self.pca.transform(embeddings)

    def fit_transform(self, X, y=None, **fit_params):
        """Encodes once, updates PCA, and returns reduced embeddings."""
        embeddings = self.encoder.encode(X, show_progress_bar=False)
        if len(embeddings) >= self.pca.n_components:
            self.pca.partial_fit(embeddings)
        return self.pca.transform(embeddings)
