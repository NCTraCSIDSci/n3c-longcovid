

@transform_pandas(
    Output(rid="ri.vector.main.execute.ed847dad-6e14-494f-8edf-eafb8ddfce41"),
    pts_for_tbl1=Input(rid="ri.foundry.main.dataset.ec39ea28-dc01-4143-ab29-31a4c52f4f56")
)
library(tidyverse)

age_dist <- function(pts_for_tbl1) {

   df <- pts_for_tbl1 
   df <- filter(df, theGroup == "Long-COVID Clinic HOSP" | theGroup == "Long-COVID Clinic NONHOSP")
   quants <- quantile(df$apprx_age, probs = c(0.01, 0.25, 0.50, 0.85, 1.0))
   plt <- ggplot(data=df, aes(df$apprx_age)) + 
    geom_histogram() +
    geom_vline(xintercept = quants)

   print(quants)
   print(plt)
return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.d40d8073-a059-4667-9160-a272452f5845"),
    features_or_query=Input(rid="ri.foundry.main.dataset.a5009cc7-ae24-4962-8122-1b73d41fadb1")
)
library(tidyverse)

oddsplot_hosp <- function(features_or_query) {
  oddsplot <-
    features_or_query %>% mutate(hosp_imp = as.numeric(hosp_imp))
  
  #hospitalized
  
  oddsplot$concept <-
    fct_reorder(oddsplot$concept, oddsplot$hosp_imp, .desc = TRUE)
  
  p_hosp <-
    top_n(oddsplot, n = 23, hosp_imp) %>%
    ggplot(oddsplot, mapping = aes(x = hosp_odds, y = reorder(subgroup, hosp_imp))) +
    geom_vline(aes(xintercept = 1), size = .25, linetype = "dashed") +
    geom_errorbarh(
      aes(xmax = hosp_upper_ci, xmin = hosp_lower_ci),
      size = .5,
      height = .2,
      color = "gray50"
    ) +
    geom_point(size = 2, color = "#D55E00") +
    theme_bw() +
    theme(panel.grid.minor = element_blank()) +
    scale_x_continuous(breaks = seq(0, 35, 5)) +
    ylab("") +
    xlab("OR (95% CI)") +
    ggtitle("Odds Ratios (Hospitalized Individuals)") +
    theme(plot.title = element_text(hjust = 0.5))
  
  p_hosp <-
    p_hosp + facet_wrap(
      . ~ concept,
      strip.position = "left",
      nrow = 20,
      scales = "free_y"
    ) + theme(
      strip.background = element_blank(),
      strip.text = element_blank(),
      axis.text = element_text(size = 7)
    )
  
  # image: svg
  p_hosp
  
  print(p_hosp)
  
  return(NULL)
  
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.33366045-5711-4efa-98ee-5c441cde1928"),
    features_or_query=Input(rid="ri.foundry.main.dataset.a5009cc7-ae24-4962-8122-1b73d41fadb1")
)
library(tidyverse)

oddsplot_nohosp <- function(features_or_query) {
  oddsplot <-
    features_or_query %>% mutate(nohosp_imp = as.numeric(nohosp_imp))
  
  #Non-Hospitalized
  
  oddsplot$concept <-
    fct_reorder(oddsplot$concept, oddsplot$nohosp_imp, .desc = TRUE)
  
  p_nohosp <-
    top_n(oddsplot, n = 23, nohosp_imp) %>%
    ggplot(oddsplot, mapping = aes(x = nonhosp_odds, y = reorder(subgroup, nohosp_imp))) +
    geom_vline(aes(xintercept = 1), size = .25, linetype = "dashed") +
    geom_errorbarh(
      aes(xmax = nonhosp_upper_ci, xmin = nonhosp_lower_ci),
      size = .5,
      height = .2,
      color = "gray50"
    ) +
    geom_point(size = 2, color = "forestgreen") +
    theme_bw() +
    theme(panel.grid.minor = element_blank()) +
    scale_x_continuous(breaks = seq(0, 35, 5)) +
    ylab("") +
    xlab("OR (95% CI)") +
    ggtitle("Odds Ratios (Non-hospitalized Individuals)") +
    theme(plot.title = element_text(hjust = 0.5))
  
  p_nohosp <-
    p_nohosp + facet_wrap(
      . ~ concept,
      strip.position = "left",
      nrow = 20,
      scales = "free_y"
    ) + theme(
      strip.background = element_blank(),
      strip.text = element_blank(),
      axis.text = element_text(size = 7)
    )
  
  # image: svg
  p_nohosp
  
  print(p_nohosp)
  
  
  return(NULL)
  
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.790c8895-54e5-4a00-a2ff-e10e850de661"),
    features_or_query=Input(rid="ri.foundry.main.dataset.a5009cc7-ae24-4962-8122-1b73d41fadb1")
)
library(tidyverse)

oddsplot_total <- function(features_or_query) {
  oddsplot <-
    features_or_query %>% mutate(total_imp = as.numeric(total_imp))
  
  # Overall
  
  oddsplot$concept <-
    fct_reorder(oddsplot$concept, oddsplot$total_imp, .desc = TRUE)
  
  p <-
    top_n(oddsplot, n = 23, total_imp) %>%
    ggplot(oddsplot, mapping = aes(x = total_odds, y = reorder(subgroup, total_imp))) +
    geom_vline(aes(xintercept = 1), size = .25, linetype = "dashed") +
    geom_errorbarh(
      aes(xmax = total_upper_ci, xmin = total_lower_ci),
      size = .5,
      height = .2,
      color = "gray50"
    ) +
    geom_point(size = 2, color = "steelblue") +
    theme_bw() +
    theme(panel.grid.minor = element_blank()) +
    scale_x_continuous(breaks = seq(0, 35, 5)) +
    ylab("") +
    xlab("OR (95% CI)") +
    ggtitle("Odds Ratios (All individuals)") + theme(plot.title = element_text(hjust = 0.5))
  
  p_total <-
    p + facet_wrap(
      . ~ concept,
      strip.position = "left",
      nrow = 20,
      scales = "free_y"
    ) + theme(strip.background = element_blank(),
              strip.text = element_blank(),
              axis.text = element_text(size = 7))
  
  
  # image: svg
  p_total
  
  print(p_total) 

  
  return(NULL)
  
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3f6e64c2-01b0-4a31-906d-2bfd35195bf7"),
    setup_or1=Input(rid="ri.foundry.main.dataset.dacbe0f6-d0cc-4173-a77a-5d791b48a039")
)
library(tidyverse)
library(data.table)
#######################
# OR Metrics (regression)

or1_calc <- function(setup_or1) {
  
  or1 <- setup_or1
  
  #Create df to store output
  concept <-
    c (
      "apprx_age",
      "apprx_age",
      "apprx_age",
      "apprx_age",
      "op_post_visit_ratio",
      "ip_post_visit_ratio"
    )
  subgroup <-
    c ("Age (26-45)",
       "Age (46-55)",
       "Age (56-65)",
       "Age (66+)",
       "op_post_visit_ratio",
       "ip_post_visit_ratio")
  total_odds <- c(1:6)
  total_upper_ci <- c(1:6)
  total_lower_ci <- c(1:6)
  hosp_odds <- c(1:6)
  hosp_upper_ci <- c(1:6)
  hosp_lower_ci <- c(1:6)
  nonhosp_odds <- c(1:6)
  nonhosp_upper_ci <- c(1:6)
  nonhosp_lower_ci <- c(1:6)
  
  odds_ratios1 <-
    data.frame(
      concept,
      subgroup,
      total_odds,
      total_upper_ci,
      total_lower_ci,
      hosp_odds,
      hosp_upper_ci,
      hosp_lower_ci,
      nonhosp_odds,
      nonhosp_upper_ci,
      nonhosp_lower_ci
    )
  

  # Variable management: Scaling ratios (such that 1 unit change corresponds to a 0.01 increase), age as a factor
  or1$ip_post_visit_ratio <- or1$ip_post_visit_ratio*10
  or1$op_post_visit_ratio <- or1$op_post_visit_ratio*10
  or1$apprx_age <- as.factor(or1$apprx_age)

  # Logistic Regression models
  
  # Total odds, total upper ci, total lower ci
  
  logit <-
    glm(
      formula = long_covid ~ op_post_visit_ratio,
      data = or1,
      family = binomial(link = "logit")
    )
  logit_coef <- coefficients(logit)
  logit_conf <- confint(logit)
  odds_ratios1$total_odds[odds_ratios1$concept == "op_post_visit_ratio"] <-
    exp(logit_coef[2])
  odds_ratios1$total_upper_ci[odds_ratios1$concept == "op_post_visit_ratio"] <-
    exp(logit_conf[4])
  odds_ratios1$total_lower_ci[odds_ratios1$concept == "op_post_visit_ratio"] <-
    exp(logit_conf[2])
  
  logit <-
    glm(
      formula = long_covid ~ ip_post_visit_ratio,
      data = or1,
      family = binomial(link = "logit")
    )
  logit_coef <- coefficients(logit)
  logit_conf <- confint(logit)
  odds_ratios1$total_odds[odds_ratios1$concept == "ip_post_visit_ratio"] <-
    exp(logit_coef[2])
  odds_ratios1$total_upper_ci[odds_ratios1$concept == "ip_post_visit_ratio"] <-
    exp(logit_conf[4])
  odds_ratios1$total_lower_ci[odds_ratios1$concept == "ip_post_visit_ratio"] <-
    exp(logit_conf[2])
  
  
  logit <-
    glm(
      formula = long_covid ~ apprx_age,
      data = or1,
      family = binomial(link = "logit")
    )
  logit_coef <- coefficients(logit)
  logit_conf <- confint(logit)
  
  odds_ratios1$total_odds[odds_ratios1$subgroup == "Age (26-45)"] <-
    exp(logit_coef[2])
  odds_ratios1$total_odds[odds_ratios1$subgroup == "Age (46-55)"] <-
    exp(logit_coef[3])
  odds_ratios1$total_odds[odds_ratios1$subgroup == "Age (56-65)"] <-
    exp(logit_coef[4])
  odds_ratios1$total_odds[odds_ratios1$subgroup == "Age (66+)"] <-
    exp(logit_coef[5])
  
  odds_ratios1$total_lower_ci[odds_ratios1$subgroup == "Age (26-45)"] <-
    exp(logit_conf[2])
  odds_ratios1$total_lower_ci[odds_ratios1$subgroup == "Age (46-55)"] <-
    exp(logit_conf[3])
  odds_ratios1$total_lower_ci[odds_ratios1$subgroup == "Age (56-65)"] <-
    exp(logit_conf[4])
  odds_ratios1$total_lower_ci[odds_ratios1$subgroup == "Age (66+)"] <-
    exp(logit_conf[5])
  
  odds_ratios1$total_upper_ci[odds_ratios1$subgroup == "Age (26-45)"] <-
    exp(logit_conf[7])
  odds_ratios1$total_upper_ci[odds_ratios1$subgroup == "Age (46-55)"] <-
    exp(logit_conf[8])
  odds_ratios1$total_upper_ci[odds_ratios1$subgroup == "Age (56-65)"] <-
    exp(logit_conf[9])
  odds_ratios1$total_upper_ci[odds_ratios1$subgroup == "Age (66+)"] <-
    exp(logit_conf[10])
  
  
  
  # Hosp odds, Hosp upper ci, Hosp lower ci
  or1_hosp <- subset(or1, hospitalized == 1)
  
  
  logit <-
    glm(
      formula = long_covid ~ op_post_visit_ratio,
      data = or1_hosp,
      family = binomial(link = "logit")
    )
  logit_coef <- coefficients(logit)
  logit_conf <- confint(logit)
  odds_ratios1$hosp_odds[odds_ratios1$concept == "op_post_visit_ratio"] <-
    exp(logit_coef[2])
  odds_ratios1$hosp_upper_ci[odds_ratios1$concept == "op_post_visit_ratio"] <-
    exp(logit_conf[4])
  odds_ratios1$hosp_lower_ci[odds_ratios1$concept == "op_post_visit_ratio"] <-
    exp(logit_conf[2])
  
  logit <-
    glm(
      formula = long_covid ~ ip_post_visit_ratio,
      data = or1_hosp,
      family = binomial(link = "logit")
    )
  logit_coef <- coefficients(logit)
  logit_conf <- confint(logit)
  odds_ratios1$hosp_odds[odds_ratios1$concept == "ip_post_visit_ratio"] <-
    exp(logit_coef[2])
  odds_ratios1$hosp_upper_ci[odds_ratios1$concept == "ip_post_visit_ratio"] <-
    exp(logit_conf[4])
  odds_ratios1$hosp_lower_ci[odds_ratios1$concept == "ip_post_visit_ratio"] <-
    exp(logit_conf[2])
  
  
  logit <-
    glm(
      formula = long_covid ~ apprx_age,
      data = or1_hosp,
      family = binomial(link = "logit")
    )
  logit_coef <- coefficients(logit)
  logit_conf <- confint(logit)
  
  odds_ratios1$hosp_odds[odds_ratios1$subgroup == "Age (26-45)"] <-
    exp(logit_coef[2])
  odds_ratios1$hosp_odds[odds_ratios1$subgroup == "Age (46-55)"] <-
    exp(logit_coef[3])
  odds_ratios1$hosp_odds[odds_ratios1$subgroup == "Age (56-65)"] <-
    exp(logit_coef[4])
  odds_ratios1$hosp_odds[odds_ratios1$subgroup == "Age (66+)"] <-
    exp(logit_coef[5])
  
  odds_ratios1$hosp_lower_ci[odds_ratios1$subgroup == "Age (26-45)"] <-
    exp(logit_conf[2])
  odds_ratios1$hosp_lower_ci[odds_ratios1$subgroup == "Age (46-55)"] <-
    exp(logit_conf[3])
  odds_ratios1$hosp_lower_ci[odds_ratios1$subgroup == "Age (56-65)"] <-
    exp(logit_conf[4])
  odds_ratios1$hosp_lower_ci[odds_ratios1$subgroup == "Age (66+)"] <-
    exp(logit_conf[5])
  
  odds_ratios1$hosp_upper_ci[odds_ratios1$subgroup == "Age (26-45)"] <-
    exp(logit_conf[7])
  odds_ratios1$hosp_upper_ci[odds_ratios1$subgroup == "Age (46-55)"] <-
    exp(logit_conf[8])
  odds_ratios1$hosp_upper_ci[odds_ratios1$subgroup == "Age (56-65)"] <-
    exp(logit_conf[9])
  odds_ratios1$hosp_upper_ci[odds_ratios1$subgroup == "Age (66+)"] <-
    exp(logit_conf[10])
  
  # Non-hosp odds, Non-hosp upper ci, Non-hosp lower ci
  or1_nonhosp <- subset(or1, hospitalized == 0)
  
  
  logit <-
    glm(
      formula = long_covid ~ op_post_visit_ratio,
      data = or1_nonhosp,
      family = binomial(link = "logit")
    )
  logit_coef <- coefficients(logit)
  logit_conf <- confint(logit)
  odds_ratios1$nonhosp_odds[odds_ratios1$concept == "op_post_visit_ratio"] <-
    exp(logit_coef[2])
  odds_ratios1$nonhosp_upper_ci[odds_ratios1$concept == "op_post_visit_ratio"] <-
    exp(logit_conf[4])
  odds_ratios1$nonhosp_lower_ci[odds_ratios1$concept == "op_post_visit_ratio"] <-
    exp(logit_conf[2])
  
  logit <-
    glm(
      formula = long_covid ~ ip_post_visit_ratio,
      data = or1_nonhosp,
      family = binomial(link = "logit")
    )
  logit_coef <- coefficients(logit)
  logit_conf <- confint(logit)
  odds_ratios1$nonhosp_odds[odds_ratios1$concept == "ip_post_visit_ratio"] <-
    exp(logit_coef[2])
  odds_ratios1$nonhosp_upper_ci[odds_ratios1$concept == "ip_post_visit_ratio"] <-
    exp(logit_conf[4])
  odds_ratios1$nonhosp_lower_ci[odds_ratios1$concept == "ip_post_visit_ratio"] <-
    exp(logit_conf[2])
  
  
  logit <-
    glm(
      formula = long_covid ~ apprx_age,
      data = or1_nonhosp,
      family = binomial(link = "logit")
    )
  logit_coef <- coefficients(logit)
  logit_conf <- confint(logit)
  
  odds_ratios1$nonhosp_odds[odds_ratios1$subgroup == "Age (26-45)"] <-
    exp(logit_coef[2])
  odds_ratios1$nonhosp_odds[odds_ratios1$subgroup == "Age (46-55)"] <-
    exp(logit_coef[3])
  odds_ratios1$nonhosp_odds[odds_ratios1$subgroup == "Age (56-65)"] <-
    exp(logit_coef[4])
  odds_ratios1$nonhosp_odds[odds_ratios1$subgroup == "Age (66+)"] <-
    exp(logit_coef[5])
  
  odds_ratios1$nonhosp_lower_ci[odds_ratios1$subgroup == "Age (26-45)"] <-
    exp(logit_conf[2])
  odds_ratios1$nonhosp_lower_ci[odds_ratios1$subgroup == "Age (46-55)"] <-
    exp(logit_conf[3])
  odds_ratios1$nonhosp_lower_ci[odds_ratios1$subgroup == "Age (56-65)"] <-
    exp(logit_conf[4])
  odds_ratios1$nonhosp_lower_ci[odds_ratios1$subgroup == "Age (66+)"] <-
    exp(logit_conf[5])
  
  odds_ratios1$nonhosp_upper_ci[odds_ratios1$subgroup == "Age (26-45)"] <-
    exp(logit_conf[7])
  odds_ratios1$nonhosp_upper_ci[odds_ratios1$subgroup == "Age (46-55)"] <-
    exp(logit_conf[8])
  odds_ratios1$nonhosp_upper_ci[odds_ratios1$subgroup == "Age (56-65)"] <-
    exp(logit_conf[9])
  odds_ratios1$nonhosp_upper_ci[odds_ratios1$subgroup == "Age (66+)"] <-
    exp(logit_conf[10])
  
  return(odds_ratios1)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e7885361-97f3-40a4-a0da-be3bee24279e"),
    setup_features_or=Input(rid="ri.foundry.main.dataset.c364f676-e9c1-4fcb-8c25-9c322f6c8734")
)
library(tidyverse)
library(data.table)

or2_calc <- function(setup_features_or) {
  
  features_odds <- setup_features_or
  
  ###########################
  # OR Metrics (2x2)
  
  or2 <-
    features_odds[, -c(1, 2, 3, 4, 5, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 939, 940)]
  
    or2$p_hospitalized <- or2$hospitalized

  or2 <-
    setDT(or2)[, lapply(.SD, sum), by = c("long_covid", "hospitalized")]
  
  # or2 <- subset(features_odds, select = -c(3, 4, 5, 6, 7, 8, 13, 14))
  
 colnames(or2)[2:7] <-
    c("hosp_ind",
      "sex_male",
      "diabetes_ind_yes",
      "kidney_ind_yes",
      "chf_ind_yes",
      "chronicpulm_ind_yes")
  
  colnames(or2)[922] <-
    c("hospitalized")

  or2 <-
    or2 %>% pivot_longer(cols = c(3:922),
                         names_to = "concept",
                         values_to = "ctpt") %>% pivot_wider(
                           names_from = c(long_covid, hosp_ind),
                           values_from = ctpt
                         )
  
  
  colnames(or2)[2:5] <-
    c("test_nonhosp_1",
      "test_hosp_1",
      "training_hosp_1",
      "training_nonhosp_1")
  
  
  # Add in the number of non-cases for Test and Training (counts based on 'counts_by_group')
  or2 <-
    or2[!(
      or2$concept == "person_id" |
        or2$concept == "apprx_age" |
        or2$concept == "tot_long_data_days" |
        or2$concept == "total_post_dx"
    ), ]
  
  
  
  odds_ratios2 <-
    or2 %>%
    mutate(test_hosp_0 = as.numeric (10304 - or2$test_hosp_1)) %>%
    mutate(test_nonhosp_0 = as.numeric (35280 - or2$test_nonhosp_1)) %>%
    mutate(test_total_1 = as.numeric(test_hosp_1 + or2$test_nonhosp_1)) %>%
    mutate(test_total_0 = as.numeric (45584 - (or2$test_hosp_1 + or2$test_nonhosp_1))) %>%
    mutate(training_hosp_0 = as.numeric (231 - or2$training_hosp_1)) %>%
    mutate(training_nonhosp_0 = as.numeric (105 - or2$training_nonhosp_1)) %>%
    mutate(training_total_0 = as.numeric (336 - (
      or2$training_hosp_1 + or2$training_nonhosp_1
    ))) %>%
    mutate(training_total_1 = as.numeric(or2$training_hosp_1 + or2$training_nonhosp_1))
  
  
  
  # Odds Ratio = Odds in exposed group / Odds in unexposed group, and CIs (without Yates' Continuity Correction)
  
  # Odds and CI functions
  odds <- function(a, b, c, d) {
    or <- as.numeric ((a / b) / (c / d))
    return(or)
  }
  
  upper_ci <- function (a, b, c, d, e) {
    upper <-
      as.numeric(exp(log(e) + (1.96 * (sqrt((1 / a) + (1 / b) + (1 / c) +
                                              (1 / d)
      )))))
    return(upper)
  }
  
  lower_ci <- function (a, b, c, d, e) {
    lower <-
      as.numeric(exp(log(e) - (1.96 * (sqrt((1 / a) + (1 / b) + (1 / c) +
                                              (1 / d)
      )))))
    return(lower)
  }
  
  # Calculating vars
  
  odds_ratios2$total_odds <-
    mapply(
      odds,
      odds_ratios2$training_total_1,
      odds_ratios2$training_total_0,
      odds_ratios2$test_total_1,
      odds_ratios2$test_total_0
    )
  
  odds_ratios2$total_upper_ci <-
    mapply(
      upper_ci,
      odds_ratios2$training_total_1,
      odds_ratios2$training_total_0,
      odds_ratios2$test_total_1,
      odds_ratios2$test_total_0,
      odds_ratios2$total_odds
    )
  
  odds_ratios2$total_lower_ci <-
    mapply(
      lower_ci,
      odds_ratios2$training_total_1,
      odds_ratios2$training_total_0,
      odds_ratios2$test_total_1,
      odds_ratios2$test_total_0,
      odds_ratios2$total_odds
    )
  
  # Odds for hospitalized
  odds_ratios2$hosp_odds <-
    mapply(
      odds,
      odds_ratios2$training_hosp_1,
      odds_ratios2$training_hosp_0,
      odds_ratios2$test_hosp_1,
      odds_ratios2$test_hosp_0
    )
  
  odds_ratios2$hosp_upper_ci <-
    mapply(
      upper_ci,
      odds_ratios2$training_hosp_1,
      odds_ratios2$training_hosp_0,
      odds_ratios2$test_hosp_1,
      odds_ratios2$test_hosp_0,
      odds_ratios2$hosp_odds
    )
  
  odds_ratios2$hosp_lower_ci <-
    mapply(
      lower_ci,
      odds_ratios2$training_hosp_1,
      odds_ratios2$training_hosp_0,
      odds_ratios2$test_hosp_1,
      odds_ratios2$test_hosp_0,
      odds_ratios2$hosp_odds
    )
  
  # Odds for non-hospitalized
  
  odds_ratios2$nonhosp_odds <-
    mapply(
      odds,
      odds_ratios2$training_nonhosp_1,
      odds_ratios2$training_nonhosp_0,
      odds_ratios2$test_nonhosp_1,
      odds_ratios2$test_nonhosp_0
    )
  
  odds_ratios2$nonhosp_upper_ci <-
    mapply(
      upper_ci,
      odds_ratios2$training_nonhosp_1,
      odds_ratios2$training_nonhosp_0,
      odds_ratios2$test_nonhosp_1,
      odds_ratios2$test_nonhosp_0,
      odds_ratios2$nonhosp_odds
    )
  
  odds_ratios2$nonhosp_lower_ci <-
    mapply(
      lower_ci,
      odds_ratios2$training_nonhosp_1,
      odds_ratios2$training_nonhosp_0,
      odds_ratios2$test_nonhosp_1,
      odds_ratios2$test_nonhosp_0,
      odds_ratios2$nonhosp_odds
    )
  
  
   
  return(odds_ratios2)
  
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.5e7cdcec-e2fd-48e4-bdba-4eb156c978ac"),
    pts_for_tbl1=Input(rid="ri.foundry.main.dataset.ec39ea28-dc01-4143-ab29-31a4c52f4f56")
)
library(tidyverse)
library(tableone)

table1 <- function(pts_for_tbl1) {

caseRws <- mutate_at(pts_for_tbl1, vars(sex_norm, race_norm, ethn_norm, age_group, diabetes_ind, kidney_ind, CHF_ind, ChronicPulm_ind, theGroup),funs(factor))

myVars <- c("sex_norm", "apprx_age", "race_norm", "ethn_norm", "age_group", "diabetes_ind", "kidney_ind", "CHF_ind", "ChronicPulm_ind", "post_visits_per_pt_day")
## Vector of categorical variables that need transformation
catVars <- c("sex_norm", "race_norm", "ethn_norm", "age_group","diabetes_ind", "kidney_ind", "CHF_ind", "ChronicPulm_ind")

tab1 <- CreateTableOne(vars = myVars, data = caseRws, factorVars = catVars, strata = "theGroup")
print(tab1, quote = TRUE, noSpaces = TRUE)

return(NULL)

}

