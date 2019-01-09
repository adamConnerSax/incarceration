Vera Incarceration Trends notes
================
Adam Conner-Sax

Data was processed from the Vera incarceration trends data-set (<https://github.com/vera-institute/incarceration_trends>) and processed (aggregations, filtering, dropping of missing values) byt the author ()

Aggregated crime rate and imprisoned per crime by urbanicity:

![](notes_files/figure-markdown_github/urbanicity-1.png)

Combo with poverty/income data

``` r
trendsWithIncome_KM <- read_csv(here("../../data/", "kMeansIncarcerationRate_vs_MedianHIByYear.csv"))
ggplot(trendsWithIncome_KM,mapping=aes(x=medianHI,y=IncarcerationRate,color=total_pop)) + geom_point() + facet_wrap(~year,ncol=3) + scale_y_continuous(limits=c(NA,0.012), labels = scales::percent) + labs(title="Incarceration Rate vs. Median Income (entire US)") + xlab("Median Household Income (scaled to median=100)")
```

![](notes_files/figure-markdown_github/income%20scatter-1.png)

``` r
ggsave("incarcerationRateAndIncome.pdf",width=8,height=11)
trendsWithIncomeByState_KM <- read_csv(here("../../data/", "kMeansIncarcerationRate_vs_MedianHIByStateAndYear.csv"))
colorado <- filter(trendsWithIncomeByState_KM,state=="CO")
ggplot(colorado,mapping=aes(x=medianHI,y=IncarcerationRate,color=total_pop)) + geom_point() + facet_wrap(~year,ncol=3) + scale_y_continuous(limits=c(NA,0.012), labels = scales::percent) + labs(title = "Incarceration Rate vs. Median Income (Colorado)") +xlab("Median Household Income (scaled to median=100)")
```

    ## Warning: Removed 32 rows containing missing values (geom_point).

![](notes_files/figure-markdown_github/income%20scatter-2.png)

``` r
ggsave("incarcerationRateAndIncomeCO.pdf",width=8,height=11)
```

    ## Warning: Removed 32 rows containing missing values (geom_point).

``` r
trendsWithIncomeByUrbanicity_KM <- read_csv(here("../../data/", "kMeansIncarcerationRate_vs_MedianHIByUrbanicityAndYear.csv"))  %>% mutate (urbanicity = factor(urbanicity))
data2014 <- filter(trendsWithIncomeByUrbanicity_KM,year=="2014") 
ggplot(data2014,mapping=aes(x=medianHI,y=IncarcerationRate,color=total_pop)) + geom_point() + facet_wrap(vars(urbanicity),ncol=2) + scale_y_continuous(limits=c(NA,0.012), labels = scales::percent) + labs(title = "Incarceration Rate vs. Median Income (2014)") + xlab("Median Household Income (scaled to median=100)")
```

![](notes_files/figure-markdown_github/income%20scatter-3.png)

``` r
ggsave("incarcerationRateAndIncome2014.pdf",width=8,height=11)
```

A quick look at the joined data...

    ## Warning: Removed 1 rows containing missing values (geom_point).

![](notes_files/figure-markdown_github/CO%20data-1.png)

    ## Warning: Removed 1 rows containing missing values (geom_point).

From call
=========

Bail reform, CO have data from open records requests (What data? Just CO?) clickable map of CO counties census cdc reform policy by location
