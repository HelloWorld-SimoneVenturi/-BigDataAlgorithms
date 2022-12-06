rm(list = ls())
library(sparklyr)
library(tidyverse) # contains ggplot2, dplyr, readr, etc...
Dir.Work <- "./" # Your path
setwd(Dir.Work)

# start in R (without spark)
N <- 10L
UtRec <- c(2, 924)
# recommendation to Users

load(file = "DataRSample.RDATA")

RDataList %>% head()
RDataList %>% nrow()

#definisco la funzione di "non appartenenza"
`%nin%` = Negate(`%in%`)

#trovo tramite un inner_join le coppie di persone con un amico in comune
TibbleFriendsOfFriends <- inner_join(x=RDataList,
                                     y=RDataList,
                                     by = c("V2"),
                                     suffix = c("", "_B")
) %>% select(-frAll_B,-V2) %>% filter(V1!=V1_B)

########
TibbleFriendsOfFriends %>% head()
########

#elimino le coppie costituite dalla stessa persona;
#rimuovo, riga per riga, le persone nella colonna V1_B che sono amiche 
#delle persone in V1; poi elimino la colonna frAll
TibbleFriendsOfFriends <- TibbleFriendsOfFriends %>%
  rowwise() %>%
  filter(V1_B %nin% frAll)  %>%
  ungroup() %>% select(-frAll)

########
TibbleFriendsOfFriends  %>% head()
########

#conto il numero di amici in comune per coppia di persone, mettendolo in nFriends
TibbleFriendsOfFriends <- TibbleFriendsOfFriends  %>% 
  group_by(V1,V1_B) %>% summarise(n = n()) %>%
  transmute(V1=V1,V3=V1_B,nFriends=n) 

# ... obtaining a tibble
TibbleFriendsOfFriends 

nNodes <- TibbleFriendsOfFriends %>%
  ungroup() %>% summarize(nN = max(V3)) %>% unlist()

SelectionR <- TibbleFriendsOfFriends %>% 
  group_by(V1) %>%
  top_n(n = N, wt = nFriends - V3/(nNodes+1)) 

SolutionR <- SelectionR %>% 
  filter(V1 %in% UtRec) %>% 
  ungroup() %>% 
  arrange(V1,-nFriends,V3)

# print
SolutionR %>% group_by(V1) %>%
  summarise(suggestions = paste(V3, collapse = ",")) 


### if OK, then move to Spark
debugMode <- FALSE
if(debugMode){
  UtRec <- c(2, 924)
  fileData <- "/dataHW1sample"
} else {
  UtRec <- c(2, 924, 8941, 31506)
  fileData <- "/dataHW1"
}
sc <- spark_connect(master = "local")

DataSet <-  spark_read_parquet(sc = sc, 
                               memory = T, # in memory
                               overwrite = T,
                               name = "friends",
                               path = paste0(Dir.Work,fileData) 
)



DataSet %>% arrange(V1,V2) %>% head()
DataSet %>% sdf_nrow()

########
DataSet %>% head()
########

TibbleFriendsOfFriends <- inner_join(x=DataSet,
                                     y=DataSet,
                                     by = c("V2"),
                                     suffix = c("", "_B")
) %>% select(-frAll_B,-V2) %>% filter(V1!=V1_B) 

TibbleFriendsOfFriends %>% sdf_register(name = "TibbleFriendsOfFriends")

TibbleFriendsOfFriends <- TibbleFriendsOfFriends %>% 
  filter(!array_contains(frAll,V1_B))

TibbleFriendsOfFriends <- TibbleFriendsOfFriends  %>% 
  group_by(V1,V1_B) %>% summarise(n = n()) %>%
  transmute(V1=V1,V3=V1_B,nFriends=n) 

Selection <-  TibbleFriendsOfFriends %>% 
  group_by(V1) %>% 
  arrange(-nFriends,V3) %>% 
  mutate(rankFriends = row_number()) %>% 
  filter(rankFriends<=N) %>%
  select(-rankFriends) %>% 
  ungroup()

Solution <- Selection %>% 
  filter(V1 %in% UtRec)  %>%
  collect() %>% # in R 
  arrange(V1,-nFriends,V3)


spark_disconnect(sc)

# print
Solution %>% group_by(V1) %>%
  summarise(suggestions = paste(V3, collapse = ",")) 

  
