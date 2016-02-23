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
* 部署目录  `hadoop@172.31.1.109``/usr/local/etc/deploy/recommend_user/run`
* 对应线上首页流用户推荐

##打卡推荐
###根据性别推荐
* 根据加入打卡的用户的性别进行打卡分类（分为男女喜欢类打卡），然后根据男女性别进行打卡推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendGoalBasedOnGender.scala)
* 部署目录`hadoop@172.31.1.109``/usr/local/etc/deploy/goal_recommend_based_on_gender/run`
* 对应线上打卡推荐

###根据已经加入打卡推荐（协同）
* 根据和用户在打卡加入上类似的用户加入的打卡进行推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendGoalBaseOnSameGoalJoinedUser.scala)
* 部署目录`hadoop@172.31.1.109``/usr/local/etc/deploy/goal_common_user_goal/run`
* 对应线上打卡推荐


##推荐卡片
###关注用户点赞的卡片
* 关注的用户点过的赞的卡片进行排序推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendCardBasedOnFolowingUserLiked.scala)
* 部署目录 `hadoop@172.31.1.109``/usr/local/etc/deploy/card_recommend_based_on_following_user_liked_with_detail/run`
* 对应线上发现页关注的用户赞过

###加入同样打卡用户卡片
* 加入同样打卡的用户的卡片进行排序推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendCardBasedOnGoalJoined.scala)
* 部署目录 `hadoop@172.31.1.109``/usr/local/etc/deploy/card_recommend_based_on_goal_with_detail/run`
* 对应线上加入同样打卡的用户的卡片

###十公里内卡片
* 用户GPS距离十公里内卡片进行排序推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendCardBasedOnNearbyHot.scala)
* 部署目录 `hadoop@172.31.1.109``/usr/local/etc/deploy/recommendNearbyHotCard/run`
* 对应线上十公里内卡片

###相似用户卡片
* 相似用户（共同粉丝数多）的卡片进行推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendSimilarUser.scala) [代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendCardForSimilarUser.scala)
* 部署目录 `hadoop@172.31.1.109``/usr/local/etc/deploy/similar_user_card_with_detail/run`
* 对应详情页共同粉丝数用户卡片

###相似标签卡片
* 用户标签的共现，Jacarrd相似度，编辑距离三项作为相似度量，取三者平均进行加权，距离越小越相似。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendSimilarTag.scala)
* 相似标签的最热卡片进行推荐。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendCardForSimilarTag.scala)
*  部署目录 `hadoop@172.31.1.109``/usr/local/etc/deploy/similar_tag_card_with_detail/run`
*  对应详情页相似标签最热卡片

###性别历史热门卡片
* 按照用户性格进行热门卡片分类，推荐给新用户。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecommendHistoryHotContextBasedOnGender.scala)
* 部署目录 `hadoop@172.31.1.109``/usr/local/etc/deploy/gender_history_hot_card/run`

##推荐标签
* 最近最常使用标签。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/recommend/RecentlyMostlyUsedTag.scala)

##周报
### 全部报告汇总

* 全部报告写成Json[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/statistics/AllReport.scala) 
* 部署目录`hadoop@172.31.1.109` `/usr/local/etc/deploy/report/allReport`

### 活跃时长
* 用户每周看视频，做Plank，记步，跑步等总时长。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/statistics/ActivityTimeReport.scala)

### 运动距离
* 用户每周距离跑步。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/statistics/DistanceReport.scala)

### 心率汇总
* 用户本周心率报告，同龄人心率报告。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/statistics/HeartRatioReport.scala)

### 步数汇总
* 用户本周步数汇总，同龄人步数情况。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/statistics/StepReport.scala) 

### 每周步数趋势
* 用户每周每日的步数运动情况。[代码](https://github.com/zhiyun168/feel-recommend/blob/master/src/main/scala/com/feel/statistics/WeeklyDayStepTrend.scala)

