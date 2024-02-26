# Code section for preprocessing steps, frequency analysis, topic modelling and sentiment analysis

This section contains two datafiles:
- "project_qatar_wc_preprocessing.ipynb" for first data cleaning and general preprocessing steps. As well as combining both corpora (guardian and aljazeera)
- "project_qatar_wc_topic_model.R" for specific preprocessing steps and calculation of frequency analysis, topic models and sentiments analysis

In the following there is an explanation about how to run these files.

## project_qatar_wc_preprocessing.ipynb
### requirements
- pandas library (if not installed use '!pip install pandas' in jupyter notebook)
- import local files: 'aljazeera_data.csv' and ''guardian_data_unprepared.csv''

## project_qatar_wc_topic_model.R
### requirements
following R packages should be loaded as written in the 'Import Packages section of the code'. If not installed, use Package installer in RStudio environment.
- library(quanteda)
- library(topicmodels)
- library(ggplot2)
- library(reshape2)
- library(pals)
- library(LDAvis)
- library("tsne")
- library(udpipe)
- library(quanteda)
- library(dplyr)
- library(tidyr)
- library(ggplot2)

local files:
- "complete_filtered_corpus.csv"
- "AFINN-111"
- "baseform_en.tsv"
- "negative-words"
- "positive-words"
- "stopwords_en"
