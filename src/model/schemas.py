CLUSTERING_RESULTS_SCHEMA = """
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    n_clusters INTEGER,
    silhouette FLOAT
"""

MODEL_PARAMETERS_SCHEMA = """
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    generation INTEGER,
    epsilon FLOAT,
    mu INTEGER,
    decay_factor FLOAT,
    fitness_score FLOAT
"""
