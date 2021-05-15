# normalize and standardize data

normalize <- function(x){
  return((x - min(x)) / (max(x)))
}

standardize <- function(x){
  return((x - mean(x)) / sd(x))
}

custom_logit <- function(data, formula, output="Odds Ratio") {
  generic_output <- glm(formula = formula,
               family = binomial(link='logit'),
               data = data)
  
  # odds ratio aka "or" interpretation:
  # the odds of y=1 are or_x3 times higher when x3 increases by one unit
  # ceteris paribus
  
  if(output = "Odds Ratio") {
    to_return <- cbind(Estimate=round(coef(generic_output), 4),
                 Odds_Ratio=round(exp(coef(generic_output),4)))
  }
  
  if (output = "General") {
    to_return(generic_output)
  }
  
  return(to_return)
}


