Vera Incarceration Trends notes
================
Adam Conner-Sax

Data was processed from the Vera incarceration trends data-set (<https://github.com/vera-institute/incarceration_trends>) and processed (aggregations, filtering, dropping of missing values) byt the author ()

Aggregated crime rate and imprisoned per crime by urbanicity:

![](notes_files/figure-markdown_github/urbanicity-1.png)

Combo with poverty/income data

``` r
trendsWithIncome <- read_csv(here("../../data/", "trendsWithPoverty.csv"))
trends1995 = filter(trendsWithIncome,year==1995)
ggplot(trends1995,mapping=aes(x=medianHI,y=IncarcerationRate)) + geom_hex()
```

![](notes_files/figure-markdown_github/income%20scatter-1.png)

From call
=========

Bail reform, CO have data from open records requests (What data? Just CO?) clickable map of CO counties census cdc reform policy by location
