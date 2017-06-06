

#################################################
## Arquivo de replicação do relatório da JBS
## Autor da replicação: Manoel Galdino
## data: 05/06/2017
#################################################

# alterando pasta
setwd("C:\\Users\\mgaldino\\2017\\Geral TB\\Relatorios\\JBS\\github\\jbs")

# importado arquivo do TSE com doações
# como preciso colocar cpf como character (em vez de numeric),
# vou colocar tudo como character

receitas <- read.table("receitas_candidatos_2014_brasil.txt", sep=";", header=T, as.is=T, colClasses = "character")

# importando arquivo com lista de cnpjs
load("lista_cnpjs.RData")

# importando dados de informações dos candidatos (eleito não eleito, deferido não etc.)
# Para isso vamos adaptar uma função do pacote electionsBR
# Quero que o cpf seja character, e não numeric

library(electionsBR)
library(dplyr)

candidate_fed1 <- function (year, ascii = FALSE, encoding = "windows-1252") 
{
  electionsBR:::test_encoding(encoding)
  electionsBR:::test_fed_year(year)
  dados <- tempfile()
  sprintf("http://agencia.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_%s.zip", 
          year) %>% download.file(dados)
  unzip(dados, exdir = paste0("./", year))
  unlink(dados)
  message("Processing the data...")
  setwd(as.character(year))
  banco <- juntaDados1(encoding)
  setwd("..")
  unlink(as.character(year), recursive = T)
  if (year < 2014) {
    names(banco) <- c("DATA_GERACAO", "HORA_GERACAO", "ANO_ELEICAO", 
                      "NUM_TURNO", "DESCRICAO_ELEICAO", "SIGLA_UF", "SIGLA_UE", 
                      "DESCRICAO_UE", "CODIGO_CARGO", "DESCRICAO_CARGO", 
                      "NOME_CANDIDATO", "SEQUENCIAL_CANDIDATO", "NUMERO_CANDIDATO", 
                      "CPF_CANDIDATO", "NOME_URNA_CANDIDATO", "COD_SITUACAO_CANDIDATURA", 
                      "DES_SITUACAO_CANDIDATURA", "NUMERO_PARTIDO", "SIGLA_PARTIDO", 
                      "NOME_PARTIDO", "CODIGO_LEGENDA", "SIGLA_LEGENDA", 
                      "COMPOSICAO_LEGENDA", "NOME_COLIGACAO", "CODIGO_OCUPACAO", 
                      "DESCRICAO_OCUPACAO", "DATA_NASCIMENTO", "NUM_TITULO_ELEITORAL_CANDIDATO", 
                      "IDADE_DATA_ELEICAO", "CODIGO_SEXO", "DESCRICAO_SEXO", 
                      "COD_GRAU_INSTRUCAO", "DESCRICAO_GRAU_INSTRUCAO", 
                      "CODIGO_ESTADO_CIVIL", "DESCRICAO_ESTADO_CIVIL", 
                      "CODIGO_NACIONALIDADE", "DESCRICAO_NACIONALIDADE", 
                      "SIGLA_UF_NASCIMENTO", "CODIGO_MUNICIPIO_NASCIMENTO", 
                      "NOME_MUNICIPIO_NASCIMENTO", "DESPESA_MAX_CAMPANHA", 
                      "COD_SIT_TOT_TURNO", "DESC_SIT_TOT_TURNO")
  }
  else {
    names(banco) <- c("DATA_GERACAO", "HORA_GERACAO", "ANO_ELEICAO", 
                      "NUM_TURNO", "DESCRICAO_ELEICAO", "SIGLA_UF", "SIGLA_UE", 
                      "DESCRICAO_UE", "CODIGO_CARGO", "DESCRICAO_CARGO", 
                      "NOME_CANDIDATO", "SEQUENCIAL_CANDIDATO", "NUMERO_CANDIDATO", 
                      "CPF_CANDIDATO", "NOME_URNA_CANDIDATO", "COD_SITUACAO_CANDIDATURA", 
                      "DES_SITUACAO_CANDIDATURA", "NUMERO_PARTIDO", "SIGLA_PARTIDO", 
                      "NOME_PARTIDO", "CODIGO_LEGENDA", "SIGLA_LEGENDA", 
                      "COMPOSICAO_LEGENDA", "NOME_COLIGACAO", "CODIGO_OCUPACAO", 
                      "DESCRICAO_OCUPACAO", "DATA_NASCIMENTO", "NUM_TITULO_ELEITORAL_CANDIDATO", 
                      "IDADE_DATA_ELEICAO", "CODIGO_SEXO", "DESCRICAO_SEXO", 
                      "COD_GRAU_INSTRUCAO", "CODIGO_COR_RACA", "DESCRICAO_COR_RACA", 
                      "DESCRICAO_GRAU_INSTRUCAO", "CODIGO_ESTADO_CIVIL", 
                      "DESCRICAO_ESTADO_CIVIL", "CODIGO_NACIONALIDADE", 
                      "DESCRICAO_NACIONALIDADE", "SIGLA_UF_NASCIMENTO", 
                      "CODIGO_MUNICIPIO_NASCIMENTO", "NOME_MUNICIPIO_NASCIMENTO", 
                      "DESPESA_MAX_CAMPANHA", "COD_SIT_TOT_TURNO", "DESC_SIT_TOT_TURNO", 
                      "EMAIL_CANDIDATO")
  }
  if (ascii == T) 
    banco <- to_ascii(banco, encoding)
  message("Done.\n")
  return(banco)
}


juntaDados1 <- function (encoding) 
{
  banco <- Sys.glob("*.txt") %>% 
    lapply(function(x) tryCatch(read.table(x, header = F, sep = ";", colClasses = "character",
                                           stringsAsFactors = F, fill = T, fileEncoding = encoding), error = function(e) NULL))
  nCols <- sapply(banco, ncol)
  banco <- banco[nCols == electionsBR:::Moda(nCols)] %>% do.call("rbind", .)
  banco
}

  
candidatura <- candidate_fed1(2014)

# selecionando apenas colunas de interesse
candidatura1 <- candidatura %>%
  select(NUM_TURNO, DESCRICAO_CARGO, CPF_CANDIDATO, NOME_URNA_CANDIDATO, DES_SITUACAO_CANDIDATURA, DESC_SIT_TOT_TURNO) %>%
  mutate(bol_2turnos = ifelse(DESC_SIT_TOT_TURNO == "ELEITO", 1,
                              ifelse(DESC_SIT_TOT_TURNO == '2º TURNO', 0,
                                     1)))

# juntando os bancos de cnpj e receitas e de resultado eleitoral
doacao <- receitas %>%
  mutate(cnpj_doadores = ifelse(CPF.CNPJ.do.doador.originário == "#NULO",
                              CPF.CNPJ.do.doador, CPF.CNPJ.do.doador.originário),
         cargo_upper = toupper(Cargo)) %>%
  left_join(lista_cnpjs, by= c("cnpj_doadores" = "CNPJ")) 

doacao1 <- candidatura1 %>%
  filter(DES_SITUACAO_CANDIDATURA %in%
           c("DEFERIDO", "DEFERIDO COM RECURSO", "INDEFERIDO COM RECURSO", "SUBSTITUTO PENDENTE DE JULGAMENTO")) %>%
  full_join(doacao, by = c("CPF_CANDIDATO" = "CPF.do.candidato", "DESCRICAO_CARGO" = "cargo_upper")) %>%
  filter(!is.na(DES_SITUACAO_CANDIDATURA)) %>% # retirando candidatos que não foram deferidos (ocorre duplicação apesar do filtro acima)
  filter(bol_2turnos == 1) #3 retirando duplicatas de candidatos que foram apara 2o turno



## Validação
## vamos agora sempre trabalhar com o banco "doacao"
# número de candidatos que receberam doação
doacao1 %>%
  filter(agrupador == "JBS") %>%
  group_by(DESCRICAO_CARGO, agrupador) %>%
  summarise(qtde= n_distinct(CPF_CANDIDATO))

doacao1 %>%
  filter(agrupador == "JBS") %>%
  mutate(Valor.receita = as.numeric(gsub(",", "\\.", Valor.receita))) %>%
  group_by(DESCRICAO_CARGO, agrupador) %>%
  summarise(receita = sum(Valor.receita))

doacao1 %>%
  filter(agrupador == "JBS", DESCRICAO_CARGO == "PRESIDENTE" ) %>%
  mutate(Valor.receita = as.numeric(gsub(",", "\\.", Valor.receita))) %>%
  group_by(DESCRICAO_CARGO, agrupador, NOME_URNA_CANDIDATO, DESC_SIT_TOT_TURNO) %>%
  summarise(receita = sum(Valor.receita))
