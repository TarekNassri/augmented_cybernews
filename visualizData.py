import pandas as pd
from gensim.corpora import Dictionary
from gensim.models import LdaModel
import pyLDAvis.gensim
from gensim.parsing.preprocessing import preprocess_string, remove_stopwords
import re



def remove_punctuation(text):
  """
  Entfernt Punkte (".") und Kommas (",") aus einem Text.

  Args:
      text (str): Der Text, aus dem die Satzzeichen entfernt werden sollen.

  Returns:
      str: Der Text ohne Punkte und Kommas.
  """
  return re.sub(r"[.,]", "", text)







# Daten laden
data = pd.read_csv( '/Users/tarek/Downloads/result_topic_modelling (1).csv')

# Tokenisierung der Textdaten und Filterung von Stoppwörtern
custom_filters = [lambda x: x.lower(), remove_stopwords]
texts = []
for text in data['text']:
    # Tokenisierung und Stoppwortfilterung
    text = remove_punctuation(text)
    words = preprocess_string(text, filters=custom_filters)
    texts.append(words)
# Erstellen des Wörterbuchs
dictionary = Dictionary(texts)

# Filtern von seltenen und häufigen Begriffen
dictionary.filter_extremes(no_below=5, no_above=0.5)

# Bag-of-Words Darstellung erstellen
corpus = [dictionary.doc2bow(text) for text in texts]

# LDA-Modell anwenden
lda_model = LdaModel(corpus, id2word=dictionary, num_topics=3, random_state=42)

# Visualisierung mit LDAvis und Speichern als HTML-Datei
vis = pyLDAvis.gensim.prepare(lda_model, corpus, dictionary)
pyLDAvis.save_html(vis, 'lda_visualization.html')



