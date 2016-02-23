#文档
##用户推荐
###协同推荐
* A, B用户很相似（共同粉丝多），如果C关注了A（或B），那么给C推荐B（或B）。 这个策略出来的用户，基本是大V。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendUserBasedOnAlsoFollowing.scala) 


###N度用户推荐
* 如果A，B用户通过关注关系能够联通，而A，B没有互相关注，那么给A，B推荐对方。保证了一定的随机性。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendUserBasedOnCCFollowing.scala)

###用户特征计算
* 计算用户年龄，用户性别，用户关注最多的标签特征。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/UserInfoData.scala) [代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/UserInfoFeature.scala) 

###用户排序推荐

* 按照已经关注的用户的平均特征相似程度排序。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RankRecommendedUser.scala)
 
### 任务提交
* 部署目录  

`hadoop@172.31.1.109``/usr/local/etc/deploy/recommend_user/run`

##打卡推荐
###根据性别推荐
* 根据加入打卡的用户的性别进行打卡分类（分为男女喜欢类打卡），然后根据男女性别进行打卡推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendGoalBasedOnGender.scala)
* 部署目录
`hadoop@172.31.1.109``/usr/local/etc/deploy/recommend_user/run`

###根据已经加入打卡推荐（协同）
根据和用户在打卡加入上类似的用户加入的打卡进行推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendGoalBaseOnSameGoalJoinedUser.scala)

##推荐卡片
###关注用户点赞的卡片
关注的用户点过的赞的卡片进行排序推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendCardBasedOnFolowingUserLiked.scala)
###加入同样打卡用户卡片
加入同样打卡的用户的卡片进行排序推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendCardBasedOnGoalJoined.scala)
###十公里内卡片
用户GPS距离十公里内卡片进行排序推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendCardBasedOnNearbyHot.scala)
###相似用户卡片
相似用户（共同粉丝数多）的卡片进行推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendCardForSimilarUser.scala)
###相似标签卡片
相似标签的最热卡片进行推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendCardForSimilarTag.scala)
###性别历史热门卡片
按照用户性格进行热门卡片分类，推荐给新用户。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendHistoryHotContextBasedOnGender.scala)
##推荐标签
用户标签的共现，Jacarrd相似度，编辑距离三项作为相似度量，取三者平均进行加权，距离越小越相似。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecentlyMostlyUsedTag.scala)



