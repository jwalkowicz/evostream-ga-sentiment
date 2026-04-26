# This program should:
#1. Consume the reduced embeddings from topic
#2. Throw it into a sentiment analyser (dimension mask is used from redis cache that will be initially set by genetic algorithm)
#3. if a given metric falls below a specific theshold (concept drift), it runs the genetic algorithm asyncrhonously to update the dimension mask in redis cache
#4. after the genetic algorithm finishes, the new dimension mask is used for the next batch of embeddings
