Dataset : Yelp Dataset : https://www.yelp.com/academic_dataset

Analyzed yelp dataset to derive useful statistics about "user”, “business" and "review" entities. Dataset was stored in Hadoop HDFS. Designed Map Reduce java programs for following concepts:

Problem 1 : Filtering complex Data : Listed business ids using business address as a filtering column

Problem 2 : Calculated average ratings for each business id and listed top 10

Problem 3 : Reduce Side Join and Job Chaining: Calculated average ratings of each business. Using these ratings listed top 10 businesses and their corresponding data.

Problem 4 : In Memory Join: Loaded all the business entities in a distributed cache. Listed user id and review text for businesses located in particular area using In Memory Join.