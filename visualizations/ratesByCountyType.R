library('tidyverse')
urbanicity <- read.csv("DataScience/incarceration/data/ratesByUrbanicityAndYear.csv")

ggplot(data=urbanicity) + geom_line(mapping=aes(x=year,y=CrimeRate,colour=urbanicity)) + geom_line(mapping=aes(x=year,y=IncarcerationRate,colour=urbanicity),linetype=2) + labs(title="Crime (solid) and Incarceration (dashed) rates by type of county", y="") + scale_y_continuous(labels=scales::percent)

#ggsave("DataScience/incarceration/visualizations/output/ratesByCountyType.pdf",width=20,height=20,units="cm")