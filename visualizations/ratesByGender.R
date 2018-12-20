library('tidyverse')
byGender <- read.csv("DataScience/incarceration/data/ratesByGenderAndYear.csv")

# all on one
allTogether <- ggplot(data=byGender) + geom_line(mapping=aes(x=year,y=IncarcerationRate,colour=Gender)) + geom_line(mapping=aes(x=year,y=PrisonAdmRate,colour=Gender),linetype=2) + labs(title="Incarceration (solid) and Admit (dashed) rates by gender", y="") + scale_y_continuous(labels=scales::percent)

# faceted
asRows <- ggplot(data=byGender) + geom_line(mapping=aes(x=year,y=IncarcerationRate,colour=Gender)) + geom_line(mapping=aes(x=year,y=PrisonAdmRate,colour=Gender),linetype=2) + labs(title="Incarceration (solid) and Admit (dashed) rates", y="") + scale_y_continuous(labels=scales::percent) + facet_grid(rows=vars(Gender))

#ggsave("DataScience/incarceration/visualizations/output/ratesByGender.pdf",width=20,height=20,units="cm")