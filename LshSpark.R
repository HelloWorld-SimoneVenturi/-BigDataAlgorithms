rm(list = ls())
library(sparklyr)
library(tidyverse) # contains ggplot2, dplyr, readr, etc...
Dir.Work <- "./"
setwd(Dir.Work)


sc <- spark_connect(master = "local")

#############
## IMPORT DataSetPoint
#############

typeCols <- paste0("V",1:400,"= 'double'",collapse = ",")
typeCols <- paste("list(",typeCols,")")

#######
# dataBase Immagini
#######
datasetImages <-  spark_read_csv(sc, name = "datasetImages",
                                 path = "lsh.csv.gz",
                                 columns = eval(parse(text = typeCols)),
                                 header = FALSE, memory = F)

###############
datasetImages %>% head()
###############

#######
# dataSet 20 Immagini z_i
#######
# In R
ImagesTestR <- read_csv(file = "lsh2.csv.gz",
                        col_names = FALSE)

names(ImagesTestR) <- paste0("V",1:400)

# In Spark
tbfImages <-  copy_to(dest = sc,
                      df = ImagesTestR,
                      name = "tbfImages",
                      memory = F)

###############
tbfImages %>% head()
###############

#assemblo le immagini
datasetImages_Assembled <- datasetImages %>% 
  ft_vector_assembler(input_cols = paste0("V",1:400),output_col = "vect") %>%
  select(vect) %>% 
  sdf_with_unique_id %>% 
  #  sdf_with_sequential_id %>% 
  mutate(id = int(id)) %>%
  sdf_register(name = "datasetImages_Assembled")

tbl_cache(sc, "datasetImages_Assembled")

#assemblo le immagini test
tbfImages_Assembled <- tbfImages %>% 
  ft_vector_assembler(input_cols = paste0("V",1:400),output_col = "vect") %>%
  select(vect) %>% 
  sdf_with_unique_id %>% 
  #  sdf_with_sequential_id %>% 
  mutate(id = int(id)) %>%
  sdf_register(name = "tbfImages_Assembled")

tbl_cache(sc, "tbfImages_Assembled")

###############
datasetImages_Assembled %>% head()
###############

###############
tbfImages_Assembled %>% head()
###############

#definisco un modello di solo or (essendo ottimizzato su Spark), da fittare a datasetImages_Assembled;
#la lunghezza dei cestini è fissata a 100, mentre il numero di hash a 24.
model_LSH <- ft_bucketed_random_projection_lsh(sc,
                                               input_col = "vect",
                                               output_col = "buckets",
                                               bucket_length = 100,
                                               num_hash_tables = 24) %>%
  ml_fit(datasetImages_Assembled)


#porto su R le immagini test
testPoints <- tbfImages_Assembled %>% 
  select(vect) %>% collect 

#creo un vettore che conterrà le distanze medie
distmean <- rep(-1,20)

#ciclo sulle immagini z_i
for (i in 1:20) {
  
  #converto come vettore l'immagine z_i 
  testpoint <- testPoints$vect[i] %>% unlist() %>% as.vector()
  
  #applico la funzione ml_approx_nearest_neighbors 
  #del modello model_LSH predisposto a datasetImages_Assembled e testpoint,
  #per determinare le prime 6 immagini simili a z_i più vicine
  #NOTA: tra queste si tiene anche la stessa z_i 
  NNeigh <- ml_approx_nearest_neighbors(model = model_LSH, 
                                        dataset = datasetImages_Assembled, 
                                        testpoint, 
                                        num_nearest_neighbors = 6 ,
                                        dist_col = "distCol")
  
  #estraggo i risultati e li ordino per distanza da z_i (prima i più vicini)
  Closest <- NNeigh %>% collect() %>% arrange(distCol)
  
  #disegno x_i*
  image(matrix(unlist(Closest$vect[1]),ncol = 20,byrow = T), axes=FALSE)
  
  #calcolo la media delle distanze
  distmean[i] <- mean(Closest$distCol)
}

distmean <- as_tibble(distmean)
View(distmean)

spark_disconnect(sc)

