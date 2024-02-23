options(stringsAsFactors = FALSE)

# ------------------------
# Import Packages
# ------------------------

library(quanteda)
library(topicmodels)
library(ggplot2)
library(reshape2)
require(pals)
library(LDAvis)
library("tsne")

# ------------------------
# Preprocessing
# ------------------------

textdata <- read.csv("grouped_filtered_df.csv", sep = ",",
                     encoding = "UTF-8")

qatar_wc_corpus <- corpus(textdata$sentence, docnames = textdata$X)

qatar_wc_corpus_sentences <- corpus_reshape(qatar_wc_corpus, to = "sentences")
ndoc(qatar_wc_corpus_sentences)

# Build a dictionary of lemmas
lemma_data <- read.csv("baseform_en.tsv",
                       encoding = "UTF-8")

# extended stopword list
stopwords_extended <- readLines("stopwords_en.txt",
                                encoding = "UTF-8")

# Preprocessing of the corpus
#-------------------------------------------------------

corpus_tokens <- qatar_wc_corpus %>%
  tokens(remove_punct = TRUE, remove_numbers = TRUE, remove_symbols = TRUE) %>%
  tokens_tolower() %>%
  tokens_replace(lemma_data$inflected_form,
                 lemma_data$lemma,
                 valuetype = "fixed") %>%
  tokens_remove(pattern = stopwords_extended, padding = T)
#calculate multi-word units 
qatar_wc_collocations <- quanteda.textstats::textstat_collocations(
  corpus_tokens,
  min_count = 25)
qatar_wc_collocations <- qatar_wc_collocations[1:197, ]#250 im original
corpus_tokens <- tokens_compound(corpus_tokens, qatar_wc_collocations)


# Preprocessing of the corpus with sentences
#----------------------------------------------
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
qatar_wc_collocations_sentences <- qatar_wc_collocations_sentences[1:250, ]#250 im original
corpus_tokens_sentences <- tokens_compound(corpus_tokens_sentences, qatar_wc_collocations_sentences)

# Preprocessing of the corpus for relevance check
#-------------------------------------------------------

corpus_tokens_relevance_check <- qatar_wc_corpus %>%
  tokens(remove_punct = TRUE, remove_numbers = TRUE, remove_symbols = TRUE) %>%
  tokens_tolower() 
#calculate multi-word units 
qatar_wc_collocations <- quanteda.textstats::textstat_collocations(
  corpus_tokens,
  min_count = 25)
qatar_wc_collocations <- qatar_wc_collocations[1:197, ]#250 im original
corpus_tokens <- tokens_compound(corpus_tokens, qatar_wc_collocations)

# ---------------------------------------------
# Creation DTM_coocurrences (for coocurrences)
# ---------------------------------------------
DTM_coocurrences <- corpus_tokens %>%
  tokens_remove("") %>%
  dfm() %>%
  dfm_trim(min_docfreq = 10) %>%
  dfm_weight("boolean")

# ---------------------------------------------
# Creation DTM Frequency (for frequency tasks)
# ---------------------------------------------

DTM_frequency <- corpus_tokens %>%
  tokens_remove("") %>%
  dfm()

# ---------------------------------------------
# Co-occurence Analysis
# ---------------------------------------------

# Matrix multiplication for cooccurrence counts
coocCounts <- t(DTM_coocurrences) %*% DTM_coocurrences

as.matrix(coocCounts[202:205, 202:205])

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
  names(sort(kij, decreasing=T)[1:20]), sort(kij, decreasing=T)[1:20],
  names(mutualInformationSig[1:20]), mutualInformationSig[1:20],
  names(dicesig[1:20]), dicesig[1:20],
  names(logsig[1:20]), logsig[1:20],
  row.names = NULL)
colnames(resultOverView) <- c(
  "Freq-terms", "Freq", "MI-terms",
  "MI", "Dice-Terms", "Dice", "LL-Terms", "LL")
print(resultOverView)
# ---------------------------------------------
# Frequency Analysis
# ---------------------------------------------

#General Frequency Alalysis

freqs <- colSums(DTM_frequency)
words <- colnames(DTM_frequency)
wordlist <- data.frame(words, freqs)
wordIndexes <- order(wordlist[, "freqs"], decreasing = TRUE)
wordlist <- wordlist[wordIndexes, ]
head(wordlist, 25)

terms_to_observe <- c("fifa","qatar","worker","work","infantino","kafala")
DTM_reduced <- as.matrix(DTM_frequency[, terms_to_observe])
counts_per_year <- aggregate(DTM_reduced,
                             by = list(year = textdata$year),
                             sum)
#visualization with matplot
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

#TF-IDF Frequency Analysis

# Compute IDF: log(N / n_i)
number_of_docs <- nrow(DTM_frequency)
term_in_docs <- colSums(DTM_frequency > 0)
idf <- log(number_of_docs / term_in_docs)
# Compute TF
tf <- as.vector(colSums(DTM_frequency))
# Compute TF-IDF
tf_idf <- tf * idf
names(tf_idf) <- colnames(DTM_frequency)

sort(tf_idf, decreasing = T)[1:20]

# ---------------------------------------------
# Creation DTM (for topic modelling)
# ---------------------------------------------

# Create DTM, but remove terms which occur in less than 1% of all documents
DTM <- corpus_tokens %>%
  tokens_remove("") %>%
  dfm() %>%
  dfm_trim(min_docfreq = 0.01, max_docfreq=0.99, docfreq_type = "prop")#0,15 und 0,5
# have a look at the number of documents and terms in the matrix

dim(DTM)

#top10_terms <- c( "unite_state", "past_year", "year_ago", "year_end", "government", "state", "country") 
#DTM <- DTM[, !(colnames(DTM) %in% top10_terms)]

# removing empty rows
sel_idx <- rowSums(DTM) > 0
DTM <- DTM[sel_idx, ]
textdata <- textdata[sel_idx, ]

# ---------------------------------------------
# Topic Modelling
# ---------------------------------------------

# number of topics
K <- 10#20
# compute the LDA model, inference via n iterations of Gibbs sampling
topicModel <- LDA(DTM, K, method="Gibbs", control=list(
  iter = 500,#500
  verbose = 25,
  seed = 1,
  alpha = 0.1))#0.1

# have a look a some of the results (posterior distributions)
tmResult <- posterior(topicModel) # posterior --> aufgrundlage der Daten erstelltes Modell
ncol(DTM) # lengthOfVocab
# topics are probability distribtions over the entire vocabulary
beta <- tmResult$terms 
# for every document we have a probability distribution of its contained topics
theta <- tmResult$topics

terms(topicModel, 10)

top5termsPerTopic <- terms(topicModel, 5)
topicNames <- apply(top5termsPerTopic, 2, paste, collapse=" ")

# ---------------------------------------------
# Expolarion of Topic Modelling results
# ---------------------------------------------

'
exampleIds <- c(2, 20, 40)
cat(textdata$Paragraph[exampleIds[1]])
cat(textdata$Paragraph[exampleIds[2]])
cat(textdata$Paragraph[exampleIds[3]])

N <- length(exampleIds)
# get topic proportions form example documents
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
topicNames <- apply(lda::top.topic.words(beta, 5, by.score = T), 2,
                    paste,
                    collapse = " ")

# most probable topics in the entire collection - via mean probablities
topicProportions <- colSums(theta) / nrow(DTM)
# assign the topic names we created before
names(topicProportions) <- topicNames
# show summed proportions in decreased order
sort(topicProportions, decreasing = TRUE)


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

# ------------------------------------------------
# Expolarion of Topic Modelling results over time
# ------------------------------------------------

# append decade information for aggregation
#textdata$year <- paste0(substr(textdata$Date, 0, 3), "0")
# get mean topic proportions per year
topic_proportion_per_year <- aggregate(theta, by = list(year = textdata$year), mean)
# set topic names to aggregated columns
colnames(topic_proportion_per_year)[2:(K+1)] <- topicNames
# reshape data frame
vizDataFrame <- melt(topic_proportion_per_year, id.vars = "year")
# plot topic proportions per deacde as bar plot

ggplot(vizDataFrame,
       aes(x=year, y=value, fill=variable)) +
  geom_bar(stat = "identity") + ylab("proportion") +
  scale_fill_manual(values = paste0(alphabet(20), "FF"), name = "year") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1),legend.text=element_text(size=6))

# ------------------------------------------------
# Analyzong quality of topic model with LDAvis
# ------------------------------------------------

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

# ------------------------------------------------
# Completing Dataframe with topics
# ------------------------------------------------

#assignment of the most propable topics to documents
most_likely_topic <- max.col(theta)
textdata$most_likely_topic <- most_likely_topic

#export of list 
my_dataframe <- as.data.frame(textdata)
write.csv(my_dataframe, "result_topic_modelling.csv", row.names = FALSE)
#current_directory <- getwd()
#print(current_directory)
