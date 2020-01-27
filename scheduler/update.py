# -*- coding: UTF-8 -*-

from offline.update_article import UpdateArticle


def update_article_profile():
    """
    定时更新文章画像的运行逻辑
    :return:
    """
    ua = UpdateArticle()
    sentence_df = ua.merge_article_data()
    if sentence_df.rdd.collect():
        textrank_keywords_df, keywordsIndex = ua.generate_article_label()
        articleProfile = ua.get_article_profile(textrank_keywords_df, keywordsIndex)
        ua.compute_article_similar(articleProfile)