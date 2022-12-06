rm(list = ls())

##### l'algoritmo è stato sviluppato su R e non su Spark
##### per il malfunzionamento della libreria graphframesG

inSpark <- FALSE
if(inSpark){
  library(graphframesG)
  source("./dense_subGraph_SPARK.R")
} else{
  source("./dense_subGraph.R")
}
  

library(igraph)
library(Matrix)
library(tidyverse) # contains ggplot2, dplyr, readr, etc...
library(sparklyr)
Dir.Work <- "./" # Your path
setwd(Dir.Work)

sc <- spark_connect(master = "local")

DataSet <-  spark_read_parquet(sc = sc, 
                               memory = T, # in memory
                               overwrite = T,
                               name = "friends",
                               path = paste0(Dir.Work,"/dataHW1") # use "/dataHW1" in final execution 
)

##### set of all verticed: already undirected (no need both V1 and V2)
vertices_tbl <- DataSet %>%
  transmute(id = V1) %>% 
  sdf_drop_duplicates()


##### already undirected V1->V2 and V2->V1 present
edges_tbl <- DataSet %>% 
  transmute(src = V1, dst = V2) %>% 
  sdf_drop_duplicates()

if(inSpark){ # code in Spark
  # DensSubGraph 
  densGrSPARK(DensGraph)
  
  Graph <- gf_graphframe(vertices = vertices_tbl, edges = edges_tbl) %>%
    gf_register()
  
  ##### compute PageRank with restart
  pageRank<- Graph %>%
    gf_pagerank(reset_prob = 0.15, 
                tol = 1e-6,
                source_id = # ... 
    )
  
  
  ##### extract vertices in R for future computing with pagerank
  PageRankinR <- pageRank$vertices %>% rename(name = id) %>% collect() %>%
    arrange(desc(pagerank))
  ##### extract edges in R for future computing
  edgR <- pageRank$edges %>% select(-weight) %>% collect()
  
  # close Spark
  spark_disconnect(sc = sc)
  
  grapH <- graph_from_data_frame(d = edgR,
                                 directed=FALSE,
                                 vertices = PageRankinR) 
  
  
} else { # code in R
  edgR <- edges_tbl %>% collect() # edges in R
  vertR <- vertices_tbl %>% collect() # vertices in R
  
  # close Spark
  spark_disconnect(sc = sc)
  
  #costruisco il grafo indiretto di lati edgR e vertici vertR
  graph <- graph_from_data_frame(d = edgR, 
                                 directed = FALSE,
                                 vertices = vertR)
  
  # DensSubGraph 
  Subgraph <- findDenseSub(vertices = vertR, edges = edgR)
  
  #calcolo le componenti del grafo graph
  Components <- components(graph)
  #trovo la componente a cui appartiene il nodo di id=1
  Comp1 <- Components$membership[which(vertR$id==1)]
  #costruisco il sottografo relativo alla componente Comp1
  SubGr <- induced_subgraph(graph,which(Components$membership==Comp1))
  #oss: ovviamente effettuare il PageRank con ripartenza, su l'intero graph
  # o solo sulla singola componente connessa SubGr a cui appartiene 
  #il nodo di ripartenza, è indifferente
  
  #calcolo le componenti del grafo SubGr
  Components2 <- components(SubGr)
  #trovo la posizione del nodo di id=1 nel sottografo SubGr 
  loc <- which(names(Components2$membership)==1)
  
  #costruisco il vettore di ripartenza S
  S = rep(x = 0,length.out = gorder(SubGr))
  S[loc] <- 1  
  
  #effettuo il PageRank con ripartenza
  PageRank <- page_rank(graph = SubGr,
                        damping = 0.85,
                        personalized = S)
  
  # index $ix of nodes from higher to lower PR 
  orderPageRank <- sort(x = PageRank$vector,
                        decreasing = TRUE, 
                        index.return = TRUE)$ix
}

#numero di nodi della comunità densa
(gorder(Subgraph))

#densità della comunità densa 
(densGr(Subgraph))

# permute order vertices according to PageRang
graphPermuted <- permute(SubGr,
                         invPerm(orderPageRank))

# get adiacency matrix. First row <-> LIN, etc...
A1 <- as_adjacency_matrix(graphPermuted, 
                          type = c("both"))


D <- rowSums(A1) # degree 
# check !!!
which((D - degree(graphPermuted)) != 0)
# Vol(Ai) = Vol(Ai-1) + di => Vol(Ai) - Vol(Ai-1) = di 
VolAi <- cumsum(D)
# Cut(Ai+1) = Cut(Ai) + di - 2 #{edg from di+1 to Ai}
# Cut(Ai+1) - Cut(Ai) =  di - 2 #{edg from di+1 to Ai}
CutAi <- cumsum(D) - 2*cumsum(rowSums(tril(A1)))

ggplot(data = tibble(Vol = VolAi,Cut = CutAi) %>%
         mutate(n = row_number()),aes(x=n,y = Vol/Cut)) +
  geom_line(aes(x = n,y = Cut/Vol)) +
  labs(x = "Node rank i in decreasing PPR score",
       y = "Conductance") +
  #  ylim(c(0,.5)) +
  theme_classic()


