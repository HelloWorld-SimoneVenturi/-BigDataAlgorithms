rm(list = ls())
library(igraph)
library(Matrix)
library(tidyverse) # contains ggplot2, dplyr, readr, etc...
library(sparklyr)

Dir.Work <- "./" # Your path
setwd(Dir.Work)

#carico i dati
load("UserShows.RDATA")

#costruisco la matrice R
R = Matrix(UserShows, sparse = TRUE) 

#calcolo le matrici P,Q,Past,Qast
P <- Diagonal(x = R %>% rowSums())
Q <- Diagonal(x = R %>% colSums())

Past <- Diagonal(x = (R %>% rowSums())^(-1/2))
Qast <- Diagonal(x = (R %>% colSums())^(-1/2))

# recall that crossprod and tcrossprod are faster and more optimized than
# t(X) %*% X and X %*% t(X), respectively

#calcolo i vettori rank per l'utente 500 in riferimento ai primi 100 spettacoli
#con il sistema di raccomandazione collaborativo utente-utente (cfuu) e oggetto-oggetto (cfoo)
vettoreRank563_cfuu <- ( Past %*%  tcrossprod(R) %*% Past %*% R)[500,1:100]
vettoreRank563_cfoo <- (R %*% Qast %*% crossprod(R) %*% Qast)[500,1:100]

#creo il grafo dalla matrice di incidenza R
GrUndir <- graph.incidence(R)

# distribution of restarting: deterministic from 500
S = rep(x = 0,length.out = gorder(GrUndir))
S[500] <- 1

#calcolo il vettore rank tramite il sistema di PageRank con ripartenza dall'utente 500 in riferimento
#ai primi 100 spettacoli
vettoreRank563 <- page_rank(graph = GrUndir,
                      personalized = S)$vector[9986:10085]

# estrazione primi 100 programmi e ordinamento indici, cfuu
suggerClaudiaUtenti<- sort(vettoreRank563_cfuu, 
                              decreasing = TRUE, 
                              index.return = T)$ix

# stampa a video primi 5 programmi suggeriti dal cfuu
cat(paste(as.vector(Shows$name[suggerClaudiaUtenti[1:5]]),collapse = "\n"))

# estrazione primi 100 programmi e ordinamento indici, cfoo
suggerClaudiaOggetti <- sort(vettoreRank563_cfoo, 
                                   decreasing = TRUE, 
                                   index.return = T)$ix

# stampa a video primi 5 programmi suggeriti dal cfoo
cat(paste(as.vector(Shows$name[suggerClaudiaOggetti[1:5]]),collapse = "\n"))

# estrazione primi 100 programmi e ordinamento indici, PageRang
suggerClaudiaPageRank <- sort(vettoreRank563, 
                              decreasing = TRUE, 
                              index.return = T)$ix

# stampa a video primi 5 programmi suggeriti dal PageRank
cat(paste(as.vector(Shows$name[suggerClaudiaPageRank[1:5]]),collapse = "\n"))

# calcolo performance: Claudia[suggerClaudiaUtenti] `e un vettore di 1 o 0 a seconda 
# che sia stato azzeccato il primo suggerimento, secondo suggerimento, ... 
Performance <- bind_rows(
  tibble(y = cumsum(Claudia[suggerClaudiaUtenti])/ seq_len(100)) %>% 
    mutate(x = row_number(), z = "user-user") ,
  tibble(y = cumsum(Claudia[suggerClaudiaOggetti])/ seq_len(100)) %>% 
    mutate(x = row_number(), z = "item-item") ,
  tibble(y = cumsum(Claudia[suggerClaudiaPageRank])/ seq_len(100)) %>%
    mutate(x = row_number(), z = "page-rank") )
# plot
ggplot(data = Performance %>% filter(x<51)) +
  geom_line(aes(y = y, x=x, color = z)) + labs(title = "Confronto di performances",
                                               x = "numero suggerimenti",
                                               y = "frazione di suggerimenti corretti") + 
  theme_classic()

