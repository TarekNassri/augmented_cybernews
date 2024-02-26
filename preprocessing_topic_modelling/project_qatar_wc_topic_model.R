options(stringsAsFactors = FALSE)

##################################################
# Import Packages
##################################################

library(quanteda)
library(topicmodels)
library(ggplot2)
library(reshape2)
library(pals)
library(LDAvis)
library("tsne")
library(udpipe)
library(quanteda)
library(dplyr)
library(tidyr)
library(ggplot2)

################################################################################
# Preprocessing
################################################################################

textdata <- read.csv("complete_filtered_corpus.csv", sep = ",",
                     encoding = "UTF-8")
textdata_guardian <- textdata[textdata$source == 'guardian', ]
textdata_aljazeera <- textdata[textdata$source == 'aljazeera', ]


qatar_wc_corpus <- corpus(textdata$text, docnames = textdata$X)

qatar_wc_corpus_sentences <- corpus_reshape(qatar_wc_corpus, to = "sentences")
ndoc(qatar_wc_corpus_sentences)

# Build a dictionary of lemmas
lemma_data <- read.csv("baseform_en.tsv",
                       encoding = "UTF-8")

# extended stopword list
stopwords_extended <- readLines("stopwords_en.txt",
                                encoding = "UTF-8")

# Model for POS-Tagging
ud_model <- udpipe_download_model(language = "english")
ud_model <- udpipe_load_model(ud_model$file_model)

# ------------------------------------------------------------------------------
# Preprocessing of the corpus with whole articles
# ------------------------------------------------------------------------------

corpus_tokens <- qatar_wc_corpus %>%
  tokens(remove_punct = TRUE, remove_numbers = TRUE, remove_symbols = TRUE) %>%
  tokens_tolower() %>%
  tokens_replace(lemma_data$inflected_form,
                 lemma_data$lemma,
                 valuetype = "fixed") %>%
  tokens_remove(pattern = stopwords_extended, padding = T)

# calculate multi-word units 
qatar_wc_collocations <- quanteda.textstats::textstat_collocations(
  corpus_tokens,
  min_count = 25)
qatar_wc_collocations <- qatar_wc_collocations[1:250, ]#250 im original
corpus_tokens <- tokens_compound(corpus_tokens, qatar_wc_collocations)


# ------------------------------------------------------------------------------
# Preprocessing of the corpus with sentences
# ------------------------------------------------------------------------------
# standard procedure with lemmatization, but for sentance based version, which is needed
# for coocurrence - analysis and defining a semantic field for the term 'fifa'

corpus_tokens_sentences <- qatar_wc_corpus_sentences %>%
  tokens(remove_punct = TRUE,
         remove_numbers = TRUE,
         remove_symbols = TRUE) %>%
  tokens_tolower() %>%
  tokens_replace(lemma_data$inflected_form,
                 lemma_data$lemma, valuetype = "fixed") %>%
  tokens_remove(pattern = stopwords_extended, padding = T)

qatar_wc_collocations_sentences <- quanteda.textstats::textstat_collocations(
  corpus_tokens_sentences,
  min_count = 25)
qatar_wc_collocations_sentences <- qatar_wc_collocations_sentences[1:250, ]
corpus_tokens_sentences <- tokens_compound(corpus_tokens_sentences, qatar_wc_collocations_sentences)

# ------------------------------------------------------------------------------
# defining nouns for pos tagging later
# ------------------------------------------------------------------------------
all_tokens <- corpus_tokens %>%
  tokens_remove("") %>%
  dfm()
term_list <- as.list(colnames(all_tokens))

check_noun <- function(term) {
  doc <- udpipe_annotate(ud_model, x = term)
  pos_tags <- as.data.frame(doc)
  return("NOUN" %in% pos_tags$upos)
}

noun_terms <- term_list[sapply(term_list, check_noun)]
noun_terms <- gsub("'", '"', noun_terms)


################################################################################
# Creation DTM (Document-Term-Matrix)
################################################################################

# ------------------------------------------------------------------------------
# Creation DTM_coocurrences
# ------------------------------------------------------------------------------
# DTM for coocurrence analysis. No POS Tagging but removing the 10% rarest terms.
# Do find suitable coocurences the sentence based version of the corpus is used

DTM_coocurrences <- corpus_tokens_sentences %>%
  tokens_remove("") %>%
  dfm() %>%
  dfm_trim(min_docfreq = 10) %>%
  dfm_weight("boolean")

# ------------------------------------------------------------------------------
# Creation DTM Frequency (for frequency tasks)
# ------------------------------------------------------------------------------
#creation of DTM without POS Tagging, and frequency based removal,
#which is used later for frequency and sentiment analysis

DTM_frequency <- corpus_tokens %>%
  tokens_remove("") %>%
  dfm()

# ------------------------------------------------------------------------------
# Creation DTM (for topic modelling)
# ------------------------------------------------------------------------------

# Create DTM 
# using POS-Tagging
# remove terms which occur in less than 2.5% of all documents and more that 99% of documents

# DTM <- tokens_keep(corpus_tokens, pattern = noun_terms) %>%
DTM <- corpus_tokens %>%
  tokens_remove("") %>%
  dfm() %>%
  dfm_trim(min_docfreq = 0.025, max_docfreq=0.99, docfreq_type = "prop")

# POS Tagging --> removing all terms, that are not nouns
DTM <- DTM[, (colnames(DTM) %in% noun_terms)]

top10_terms <- c( "world_cup", "qatar", "fifa", "football", "tournament", "year","team", "time","game", "country") 

top20_terms <- c( "world_cup", "qatar", "fifa", "football", "tournament", "year","team", "time","game", "country","player", "world", "report", "worker", "human", "fan", "people","sport", "match", "day") 

# removing top 10 terms 
DTM <- DTM[, !(colnames(DTM) %in% top10_terms)]

playing_countries <- c("australia", "iran", "japan", "qatar", "saudi arabia", "south korea", 
                       "cameroon", "ghana", "morocco", "senegal", "tunisia", "canada", 
                       "costa rica", "mexico", "united states", "argentina", "brazil", 
                       "ecuador", "uruguay", "belgium", "croatia", "denmark", "england", 
                       "france", "germany", "netherlands", "poland", "portugal", "serbia", 
                       "spain", "switzerland", "wales")

# removing the countries playing in the world cup to obtain better interpretable results
DTM <- DTM[, !(colnames(DTM) %in% playing_countries)]
dim(DTM)

# removing empty rows
sel_idx <- rowSums(DTM) > 0
DTM <- DTM[sel_idx, ]
textdata <- textdata[sel_idx, ]


################################################################################
# Co-occurence Analysis for term 'fifa' to create a semantic field
################################################################################

# Matrix multiplication for cooccurrence counts
coocCounts <- t(DTM_coocurrences) %*% DTM_coocurrences

coocTerm <- "fifa"
k <- nrow(DTM_coocurrences)
ki <- sum(DTM_coocurrences[, coocTerm])
kj <- colSums(DTM_coocurrences)
names(kj) <- colnames(DTM_coocurrences)
kij <- coocCounts[coocTerm, ]

########## MI: log(k*kij / (ki * kj) ########
mutualInformationSig <- log(k * kij / (ki * kj))
mutualInformationSig <- mutualInformationSig[order(
  mutualInformationSig,
  decreasing = TRUE)]
########## DICE: 2 X&Y / X + Y ##############
dicesig <- 2 * kij / (ki + kj)
dicesig <- dicesig[order(dicesig, decreasing=TRUE)]
########## Log Likelihood ###################
logsig <- 2 * ((k * log(k)) - (ki * log(ki)) - (kj * log(kj)) + (kij * log(kij))
               + (k - ki - kj + kij) * log(k - ki - kj + kij)
               + (ki - kij) * log(ki - kij) + (kj - kij) * log(kj - kij)
               - (k - ki) * log(k - ki) - (k - kj) * log(k - kj))
logsig <- logsig[order(logsig, decreasing=T)]


# Put all significance statistics in one Data-Frame
resultOverView <- data.frame(
  names(sort(kij, decreasing=T)[1:30]), sort(kij, decreasing=T)[1:30],
  names(mutualInformationSig[1:30]), mutualInformationSig[1:30],
  names(dicesig[1:30]), dicesig[1:30],
  names(logsig[1:30]), logsig[1:30],
  row.names = NULL)
colnames(resultOverView) <- c(
  "Freq-terms", "Freq", "MI-terms",
  "MI", "Dice-Terms", "Dice", "LL-Terms", "LL")
print(resultOverView)

################################################################################
# Frequency Analysis
################################################################################

# ---------------------------------------------
#General Frequency Alalysis
# ---------------------------------------------
freqs <- colSums(DTM)
words <- colnames(DTM)
wordlist <- data.frame(words, freqs)
wordIndexes <- order(wordlist[, "freqs"], decreasing = TRUE)
wordlist <- wordlist[wordIndexes, ]
head(wordlist, 25)

terms_to_observe <- c("fifa","qatar","worker","work","infantino","kafala")
DTM_reduced <- as.matrix(DTM_frequency[, terms_to_observe])
counts_per_year <- aggregate(DTM_reduced,
                             by = list(year = textdata$year),
                             sum)
# visualization with matplot
year <- counts_per_year$year
frequencies <- counts_per_year[, terms_to_observe]
matplot(year, frequencies, type = "l")
# add legend to the plot
l <- length(terms_to_observe)
legend('topleft', legend = terms_to_observe, col=1:l, text.col = 1:l, lty = 1:l)

# #visualization with ggplot2
df_freq_ggplot <- data.frame(year = counts_per_year$year, counts_per_year[, terms_to_observe])
# Melt die Daten, um sie für ggplot2 geeignet zu machen
df_freq_ggplot_melted <- reshape2::melt(df_freq_ggplot, id.vars = "year")

ggplot(df_freq_ggplot_melted, aes(x = year, y = value, color = variable)) +
  geom_line() +
  labs(title = "Term Frequencies Over Years",
       x = "Year",
       y = "Frequency") +
  scale_color_manual(values = 1:length(terms_to_observe)) +
  theme_minimal() +
  theme(legend.position = "top") +
  guides(color = guide_legend(title = "Terms"))

# ---------------------------------------------
#TF-IDF Frequency Analysis
# ---------------------------------------------

# Compute IDF: log(N / n_i)
number_of_docs <- nrow(DTM_frequency)
term_in_docs <- colSums(DTM_frequency > 0)
idf <- log(number_of_docs / term_in_docs)
# Compute TF
tf <- as.vector(colSums(DTM_frequency))
# Compute TF-IDF
tf_idf <- tf * idf
names(tf_idf) <- colnames(DTM_frequency)

sort(tf_idf, decreasing = T)[1:50]


################################################################################
# Topic Modelling
################################################################################

# number of topics
K <- 20
# compute the LDA model, inference via n iterations of Gibbs sampling
topicModel <- LDA(DTM, K, method="Gibbs", control=list(
  iter = 500,
  verbose = 25,
  seed = 3,
  alpha = 0.3))

# storing results in variable
tmResult <- posterior(topicModel) # posterior --> aufgrundlage der Daten erstelltes Modell

# storing probability distribtions of vocabulary for the topics
beta <- tmResult$terms 

# storing probability distribution for every documente of its contained topics
theta <- tmResult$topics
filtered_theta_guardian <- theta[textdata$source == 'guardian', ]
filtered_theta_aljazeera <- theta[textdata$source == 'aljazeera', ]

terms(topicModel, 10)

#top5termsPerTopic <- terms(topicModel, 5)
#topicNames <- apply(top5termsPerTopic, 2, paste, collapse=" ")

#--------------------------------------------------
# Expolarion of Topic Modelling results
#--------------------------------------------------


# the following commented out lines are not used, but could be helpful for further
# analysis of the topic proportions for some example documents
'
exampleIds <- c(2, 20, 40)
cat(textdata$Paragraph[exampleIds[1]])
cat(textdata$Paragraph[exampleIds[2]])
cat(textdata$Paragraph[exampleIds[3]])

N <- length(exampleIds)
topicProportionExamples <- theta[exampleIds,]
colnames(topicProportionExamples) <- topicNames
vizDataFrame <- melt(cbind(
  data.frame(topicProportionExamples),
  document = factor(1:N)),
  variable.name = "topic", id.vars = "document")
ggplot(data = vizDataFrame,
       aes(topic, value, fill = document), ylab = "proportion") +
  geom_bar(stat="identity") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  coord_flip() +
  facet_wrap(~ document, ncol = N)
'

# re-rank top topic terms for topic names
'
topicNames <- apply(lda::top.topic.words(beta, 5, by.score = T), 2,
                    paste,
                    collapse = " ")
'

# Because a concrete model is already chosen, the topicnames are customized 
# based on the LDAvis tool and the further information about relevance of terms.
# If new models are trained, this name assignement code should be commented out 
# and the top5termsPerTopic above should be used instead.

topicNames[1]= "squad player coach"
topicNames[2]= "russia gouvernment ukraine"
topicNames[3]= "worker wage amnesty"
topicNames[4]= "day thing today"
topicNames[5]= "people technology var"
topicNames[6]= "woman gay armband lgbtq"
topicNames[7]= "statement organisation letter"
topicNames[8]= "power sense sportswashing"
topicNames[9]= "stadium build host"
topicNames[10]= "bribe investigation charge warner"
topicNames[11]= "fan ticket match beer"
topicNames[12]= "security detain police activist"
topicNames[13]= "goal score minute"
topicNames[14]= "group group_stage knockout_stage playoff"
topicNames[15]= "club season champions_league"
topicNames[16]= "sport sponsorship investment"
topicNames[17]= "blatter bid vote ethic_commitee"
topicNames[18]= "winter summer schedule"
topicNames[19]= "host arab region"
topicNames[20]= "journalism issue_surround_qatar treatment"


# most probable topics - via mean probablities
topicProportions <- colSums(theta) / nrow(DTM)
names(topicProportions) <- topicNames
sort(topicProportions, decreasing = TRUE)

topicProportions_aljazeera <- colSums(filtered_theta_aljazeera) / nrow(textdata_aljazeera)
names(topicProportions_aljazeera) <- topicNames
sort(topicProportions_aljazeera, decreasing = TRUE)

topicProportions_guardian <- colSums(filtered_theta_guardian) / nrow(textdata_guardian)
names(topicProportions_guardian) <- topicNames
sort(topicProportions_guardian, decreasing = TRUE)


# most probable topics in the entire collection - via Rank-1
countsOfPrimaryTopics <- rep(0, K)
names(countsOfPrimaryTopics) <- topicNames
for (i in 1:nrow(DTM)) {
  # select topic distribution for document i
  topicsPerDoc <- theta[i, ]
  # get first element position from ordered list
  primaryTopic <- order(topicsPerDoc, decreasing = TRUE)[1]
  countsOfPrimaryTopics[primaryTopic] <- countsOfPrimaryTopics[primaryTopic] + 1
}
sort(countsOfPrimaryTopics, decreasing = TRUE)


# most probable topics guardian - via Rank-1
countsOfPrimaryTopics_guardian <- rep(0, K)
names(countsOfPrimaryTopics_guardian) <- topicNames
for (i in 1:nrow(DTM[textdata$source == "guardian", ])) {
  # select topic distribution for document i
  topicsPerDoc <- filtered_theta_guardian[i, ]
  # get first element position from ordered list
  primaryTopic <- order(topicsPerDoc, decreasing = TRUE)[1]
  countsOfPrimaryTopics_guardian[primaryTopic] <- countsOfPrimaryTopics_guardian[primaryTopic] + 1
}
sort(countsOfPrimaryTopics_guardian, decreasing = TRUE)


# most probable topics al jazeera - via Rank-1
countsOfPrimaryTopics_aljazeera <- rep(0, K)
names(countsOfPrimaryTopics_aljazeera) <- topicNames
for (i in 1:nrow(DTM[textdata$source == "aljazeera", ])) {
  # select topic distribution for document i
  topicsPerDoc <- filtered_theta_aljazeera[i, ]
  # get first element position from ordered list
  primaryTopic <- order(topicsPerDoc, decreasing = TRUE)[1]
  countsOfPrimaryTopics_aljazeera[primaryTopic] <- countsOfPrimaryTopics_aljazeera[primaryTopic] + 1
}
sort(countsOfPrimaryTopics_aljazeera, decreasing = TRUE)


# ------------------------------------------------------------------------------
# Expolarion of Topic Modelling results over time
# ------------------------------------------------------------------------------

#by changing the commented out lines about 'different topic_proportion_per_year' the plot 
#can be filtered on aljazeera data, guardian data or the whole dataset

#topic_proportion_per_year <- aggregate(filtered_theta_guardian, by = list(year = textdata_guardian$year), mean)
#topic_proportion_per_year <- aggregate(filtered_theta_aljazeera, by = list(year = textdata_aljazeera$year), mean)
topic_proportion_per_year <- aggregate(theta, by = list(year = textdata$year), mean)

colnames(topic_proportion_per_year)[2:(K+1)] <- topicNames
vizDataFrame <- melt(topic_proportion_per_year, id.vars = "year")

# plot topic proportions per year as bar plot
ggplot(vizDataFrame,
       aes(x=year, y=value, fill=variable)) +
  geom_bar(stat = "identity") + ylab("proportion") +
  scale_fill_manual(values = paste0(alphabet(20), "FF"), name = "year") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),
        legend.text=element_text(size=8),
        plot.title = element_text(hjust = 0.5, size = 25)) +
  ggtitle("Topic distribution over time")

# ------------------------------------------------------------------------------
# Analyzing quality of topic model with LDAvis
# ------------------------------------------------------------------------------

svd_tsne <- function(x) tsne(svd(x)$u)
json <- createJSON(
  phi = beta,
  theta = theta,
  doc.length = rowSums(DTM),
  vocab = colnames(DTM),
  term.frequency = colSums(DTM),
  mds.method = svd_tsne,
  plot.opts = list(xlab="", ylab="")
)
serVis(json)

################################################################################
# coocurences of topics to terms 'fifa', 'qatar' und 'wordl_cup'
# (to ensure that our topics have something to do with the investigated subject)
################################################################################

sum_coccurrences_topic <- numeric(K)

for (i in 1:K) {
  # Extrahiere die aktuellen Terme für das aktuelle Thema
  current_terms <- c(lda::top.topic.words(beta, 5, by.score = TRUE)[, i])
  
  # Extrahiere die Co-Occurrences für die Begriffe "fifa", "qatar" und "world_cup"
  coccurrences_fifa <- coocCounts[c("fifa", current_terms), current_terms]
  coccurrences_qatar <- coocCounts[c("qatar", current_terms), current_terms]
  coccurrences_wc <- coocCounts[c("world_cup", current_terms), current_terms]
  
  # Berechne die Summe der Co-Occurrences für das aktuelle Thema
  sum_coccurrences_topic[i] <- sum(coccurrences_fifa[1,]) + sum(coccurrences_qatar[1,]) + sum(coccurrences_wc[1,])
  
  cat(paste("Die 5 Häufigsten Terme von Thema", i, 'hab zu (fifa,qatar,world_cup) eine Gesamt-cooccurrence von', sum_coccurrences_topic[i], '\n'))
}


################################################################################
#Log-Likelihood-coocurrences of topics to terms 'fifa', 'qatar' und 'wordl_cup'
# (to ensure that our topics have something to do with the investigated subject)
################################################################################
sum_LL_topic <- numeric(K)

calculate_log_likelihood <- function(coocTerm, terms) {
  # Matrix multiplication for cooccurrence counts
  coocCounts <- t(DTM_coocurrences) %*% DTM_coocurrences
  
  k <- nrow(DTM_coocurrences)
  ki <- sum(DTM_coocurrences[, coocTerm])
  kj <- colSums(DTM_coocurrences)
  names(kj) <- colnames(DTM_coocurrences)
  kij <- coocCounts[coocTerm, ]
  
  ########## Log Likelihood ###################
  logsig <- 2 * ((k * log(k)) - (ki * log(ki)) - (kj * log(kj)) + (kij * log(kij))
                 + (k - ki - kj + kij) * log(k - ki - kj + kij)
                 + (ki - kij) * log(ki - kij) + (kj - kij) * log(kj - kij)
                 - (k - ki) * log(k - ki) - (k - kj) * log(k - kj))
  
  logsig <- logsig[order(logsig, decreasing = TRUE)]
  logsig <- sum(logsig[terms])
  return(logsig)
}


for (i in 1:K) {
  
  coocTerm <- "fifa"
  terms=c(lda::top.topic.words(beta, 5, by.score = TRUE)[, i])
  LL_sum_fifa <- calculate_log_likelihood(coocTerm, terms)
  
  coocTerm <- "qatar"
  terms=c(lda::top.topic.words(beta, 5, by.score = TRUE)[, i])
  LL_sum_qatar <- calculate_log_likelihood(coocTerm, terms)
  
  coocTerm <- "world_cup"
  terms=c(lda::top.topic.words(beta, 5, by.score = TRUE)[, i])
  LL_sum_wc <- calculate_log_likelihood(coocTerm, terms)
  
  sum_LL_topic[i] <- LL_sum_fifa + LL_sum_qatar + LL_sum_wc
  
  cat(paste("Die 5 häufigsten Terme von Thema", i, 'haben einen Gesamt-LL-Value zu (fifa,qatar,world_cup) von', sum_LL_topic[i], '\n'))

}

################################################################################
# Completing and exporting textdata with calculated topics
################################################################################

#assignment of the most propable topics to documents
most_likely_topic <- max.col(theta)
textdata$most_likely_topic_name <- topicNames[most_likely_topic]
textdata$most_likely_topic_number <- most_likely_topic
#export of list 
my_dataframe <- as.data.frame(textdata)
write.csv(my_dataframe, "result_topic_modelling.csv", row.names = FALSE)

################################################################################
# Sentiment Analysis
################################################################################

# by changing the commented out lines about 'DTM_sentiments' and 'textdata_sentiments '
# the plot can be filtered on aljazeera data, guardian data or the whole dataset

DTM_sentiments=DTM_frequency
textdata_sentiments <- textdata


#DTM_sentiments=DTM_frequency[textdata$source == "aljazeera", ]
#textdata_sentiments <- textdata %>%
#  filter(source == "aljazeera")

#DTM_sentiments=DTM_frequency[textdata$source == "guardian", ]
#textdata_sentiments <- textdata %>%
#  filter(source == "guardian")

# following 2 different sentiment dictionaries are available, for now Lexicon by Hu et al. is used

# English Opinion Word Lexicon by Hu et al. 2004
positive_terms_all <- readLines("positive-words.txt")
negative_terms_all <- readLines("negative-words.txt")

# AFINN sentiment lexicon by Nielsen 2011
afinn_terms <- read.csv("AFINN-111.txt", header = F, sep = "\t")
#positive_terms_all <- afinn_terms$V1[afinn_terms$V2 > 0]
#negative_terms_all <- afinn_terms$V1[afinn_terms$V2 < 0]

positive_terms_in_corpus <- intersect(colnames(DTM_sentiments), positive_terms_all)
counts_positive <- rowSums(DTM_sentiments[, positive_terms_in_corpus])
negative_terms_in_corpus <- intersect(colnames(DTM_sentiments), negative_terms_all)
counts_negative <- rowSums(DTM_sentiments[, negative_terms_in_corpus])

counts_all_terms <- rowSums(DTM_sentiments)
relative_sentiment_frequencies <- data.frame(
  positive = counts_positive / counts_all_terms,
  negative = counts_negative / counts_all_terms
)

#-------------------------------------------------------------------------------
#positive and negative sentiments per topic 
#-------------------------------------------------------------------------------

sentiments_per_topic <- relative_sentiment_frequencies %>%
  mutate(topic = textdata_sentiments$most_likely_topic_name, year = textdata_sentiments$year) %>%
  group_by(topic) %>%
  summarise(mean_positive = mean(positive,na.rm = TRUE),
            mean_negative = mean(negative,na.rm = TRUE))
head(sentiments_per_topic)


df <- sentiments_per_topic %>% pivot_longer(!topic)
ggplot(data = df, aes(x = topic, y = value, fill = name)) +
  geom_bar(stat="identity", position=position_dodge()) + coord_flip()

#-------------------------------------------------------------------------------
#relative proportion of positive and negative sentiments per topic and per year
#-------------------------------------------------------------------------------

# following the code for plotting sentiment analysis grouped by year or by topic  

sentiments_per_topic <- relative_sentiment_frequencies %>%
  mutate(topic = textdata_sentiments$most_likely_topic_name, year = textdata_sentiments$year) %>%
  group_by(topic) %>%
  summarise(pos_sentiments = mean(positive,na.rm = TRUE)/(mean(positive,na.rm = TRUE)+mean(negative,na.rm = TRUE)),
            neg_sentiments = mean(negative,na.rm = TRUE)/(mean(positive,na.rm = TRUE)+mean(negative,na.rm = TRUE)))
head(sentiments_per_topic)

sentiments_per_year <- relative_sentiment_frequencies %>%
  mutate( year = textdata_sentiments$year) %>%
  group_by(year) %>%
  summarise(pos_sentiments = mean(positive,na.rm = TRUE)/(mean(positive,na.rm = TRUE)+mean(negative,na.rm = TRUE)),
            neg_sentiments = mean(negative,na.rm = TRUE)/(mean(positive,na.rm = TRUE)+mean(negative,na.rm = TRUE)))
head(sentiments_per_year)


df_year <- sentiments_per_year %>%
  pivot_longer(!year, names_to = "sentiment_type", values_to = "value")

df_topic <- sentiments_per_topic %>%
  pivot_longer(!topic, names_to = "sentiment_type", values_to = "value")

#plotting both ggplots
ggplot(data = df_topic, aes(x = topic, y = value, fill = sentiment_type)) +
  geom_bar(stat = "identity", position = "stack") +
  geom_hline(yintercept = 0.5, color = "black", linetype = "dashed", size = 1) +
  coord_flip() +
  labs(title = "Positive and negative sentiments per topic",
       x = "Topic",
       y = "Sentiments") +
  scale_fill_manual(values = c("pos_sentiments" = "green", "neg_sentiments" = "red")) +
  theme_minimal()+
  theme(plot.title = element_text(size = 20, hjust = 0.5))

ggplot(data = df_year, aes(x = year, y = value, fill = sentiment_type)) +
  geom_bar(stat = "identity", position = "stack") +
  geom_hline(yintercept = 0.5, color = "black", linetype = "dashed", size = 1) +
  coord_flip() +
  labs(title = "Positive and negative sentiments per year",
       x = "Topic",
       y = "Sentiments") +
  scale_fill_manual(values = c("pos_sentiments" = "green", "neg_sentiments" = "red")) +
  theme_minimal()+
  theme(plot.title = element_text(size = 20, hjust = 0.5))

