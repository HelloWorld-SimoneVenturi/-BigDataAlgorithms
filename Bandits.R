rm(list = ls())

dir.work <- "./"
setwd(dir.work)
require(tidyverse) # contains ggplot2, dplyr, readr, etc...
source('./pullArm.R')

sdtart <- -10L # setting starting seed as 
nArmsEachGame <- 4L
numTrials <- 10000L

# name strategies = name objects
ArmEpsGreedy <- "ArmEpsGr"
ArmUppConSam <- "ArmUCB1"
ArmThoSam <- "ArmTS"

if(exists(ArmEpsGreedy)){
  rm(list = c(ArmEpsGreedy))
}
if(exists(ArmUppConSam)){
  rm(list = c(ArmUppConSam))
}
if(exists(ArmThoSam)){
  rm(list = c(ArmThoSam))
}

TSTab <- as_tibble(matrix(1,nrow = nArmsEachGame, ncol = 2 ))
colnames(TSTab) <- c("S","F")
theta <- as_tibble(matrix(0,nrow = nArmsEachGame, ncol = 1 ))
colnames(theta) <- c("t")
theta <- theta %>% rowid_to_column("numArm")

# initialization: one bandit for each arm for ArmEpsGreedy and ArmUppConSam
for(j in seq_len(nArmsEachGame)){
  tmp <- pullArm(namePull = ArmEpsGreedy,
                 nArm = j,
                 nPlayGame = 1,
                 seed_start = sdtart) 
  
  tmpUCB1 <- pullArm(namePull = ArmUppConSam,
                     nArm = j,
                     nPlayGame = 1,
                     seed_start = sdtart)  

}

#ciclo sulle prove
for(j in seq_len(numTrials)){
  if(j %% 100 == 0){
    cat("Num trials ",j,"out of",numTrials,"\n")
  }

  # EpsGreedyAlgorithm
  exploration <- runif(1) < 4/j
  if(exploration){
    tmp <- pullArm(namePull = ArmEpsGreedy,
                   nArm = sample.int(nArmsEachGame,size = 1),
                   nPlayGame = 1,
                   seed_start = sdtart) 
  } else {
    #cerco la media corrente più alta
    bestSoFar <- get(ArmEpsGreedy)$stats %>% 
      filter(meanArmSoFar == max(meanArmSoFar)) 
    
    #se ce ne sono più di una, ne prendo una a caso
    if( length(bestSoFar$numArm)!= 1 ){
      choice <- sample(bestSoFar$numArm,size = 1)
    }
    else
      choice <- bestSoFar$numArm
      
    
    tmp <- pullArm(namePull = ArmEpsGreedy,
                   nArm = choice,
                   nPlayGame = 1,
                   seed_start = sdtart)
  }
  
  # Upper confidence sampling algorithm
  
  #calcolo l'estremo superiore degli intervalli di confidenza
  #e prendo il maggiore
  UCS <- get(ArmUppConSam)$stats %>% rowwise() %>%transmute(UCI = meanArmSoFar + 0.05*sqrt(2*log(j)/numPlayed)) %>% ungroup() %>% rowid_to_column("numArm") 
  bestSoFarUCB1 <- UCS %>% 
    filter(UCI == max(UCI))
  
  #se ce ne sono più di uno, ne prendo uno a caso
  if( length(bestSoFarUCB1$numArm)!= 1 )
    choice <- sample(bestSoFarUCB1$numArm,size = 1)
  else
    choice <- bestSoFarUCB1$numArm
  
  tmp <- pullArm(namePull = ArmUppConSam,
                 nArm = choice,
                 nPlayGame = 1,
                 seed_start = sdtart)
  
  # Thompson Sampling algorithm
  
  for(j in seq_len(nArmsEachGame)){
  
    #calcolo le realizzazioni dei theta
    theta[j,"t"] <- rbeta(n = 1,shape1 = as.numeric(TSTab[j,"S"]), shape2 = as.numeric(TSTab[j,"F"]) )
    #prendo il maggiore
    thetafin <- theta %>% 
      filter(t == max(t))
  }
  
  # se ce ne sono più di uno, ne prendo uno a caso
  if( length(thetafin$numArm)!= 1 )
    choice <- sample(thetafin$numArm,size = 1)
  else
    choice <- thetafin$numArm
  
  tmp <- pullArm(namePull = ArmThoSam,
                   nArm = choice,
                   nPlayGame = 1,
                   seed_start = sdtart)
    
  #calcolo realizzazione della bernoulli di parametro tmp e aggiorno TSTab
  if(rbernoulli(1,tmp)){
      
    TSTab[choice,"S"] <- TSTab[choice,"S"] + 1
      
  }
  else
    TSTab[choice,"F"] <- TSTab[choice,"F"] + 1
      
}


# plot time series

seqEpdGreedy <- get(ArmEpsGreedy)$sequence %>%
  rowid_to_column("numGame") %>%
  mutate(meanRewards = cumsum(rewards)/numGame) %>%
  add_column(strategy = ArmEpsGreedy)

# other strategies

seqArmUppCOnSam <- get(ArmUppConSam)$sequence %>%
  rowid_to_column("numGame") %>%
  mutate(meanRewards = cumsum(rewards)/numGame) %>%
  add_column(strategy = ArmUppConSam)

seqThoSam <- get(ArmThoSam)$sequence %>%
  rowid_to_column("numGame") %>%
  mutate(meanRewards = cumsum(rewards)/numGame) %>%
  add_column(strategy = ArmThoSam)

dataTimeSeries <- bind_rows(seqEpdGreedy,seqArmUppCOnSam,seqThoSam) %>%
  mutate(numGame = numGame -nArmsEachGame) %>%
  filter(numGame > 0) 


ggplot(data = dataTimeSeries,mapping = aes(x = numGame,
                                           y = meanRewards)) + 
  geom_line(aes(color = strategy, linetype = strategy)) +
  ylab("mean of Rewards")+
  xlab("number of games")

