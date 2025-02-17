--所有语言
--对应
--英语 (English)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = cy_GB') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = en_GB') AS lower_case_string ;


SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = English') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = ENGLISH_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XENGLISH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = ENGLISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = ENGLISH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XENGLISH_AI') AS lower_case_string ;
--美国(American)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = AMERICAN') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = AMERICAN_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XAMERICAN') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = AMERICAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = AMERICAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XAMERICAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XAMERICAN_CI') AS lower_case_string ;
--德语 (German)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = de_DE') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = de_AT') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = de_LU') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = fy_DE') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = hsb_DE') AS lower_case_string ;

SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = GERMAN_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = XGERMAN') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = GERMAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = GERMAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = XGERMAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = XGERMAN_AI') AS lower_case_string ;
--法语 (French)
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = fr_FR') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = ca_FR') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = fr_CH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = fr_CA') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = fr_LU') AS lower_case_string ;

SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = FRENCH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = FRENCH_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = XFRENCH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = FRENCH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = FRENCH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = XFRENCH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = XFRENCH_AI') AS lower_case_string ;
--冰岛
SELECT NLS_LOWER('AaBbDdÐðEeFfGgHhIiJjKkLlMmNnOoPpRrSsTtUuVvXxYyÞþÄäÖöÁáÝýÚúÍíÉéÓóÆæÐðÞþ', 'NLS_SORT = is_IS') AS lower_case_string;


SELECT NLS_LOWER('AaBbDdÐðEeFfGgHhIiJjKkLlMmNnOoPpRrSsTtUuVvXxYyÞþÄäÖöÁáÝýÚúÍíÉéÓóÆæÐðÞþ', 'NLS_SORT = ICELANDIC') AS lower_case_string ;
SELECT NLS_LOWER('AaBbDdÐðEeFfGgHhIiJjKkLlMmNnOoPpRrSsTtUuVvXxYyÞþÄäÖöÁáÝýÚúÍíÉéÓóÆæÐðÞþ', 'NLS_SORT = ICELANDIC_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbDdÐðEeFfGgHhIiJjKkLlMmNnOoPpRrSsTtUuVvXxYyÞþÄäÖöÁáÝýÚúÍíÉéÓóÆæÐðÞþ', 'NLS_SORT = XICELANDIC') AS lower_case_string ;
SELECT NLS_LOWER('AaBbDdÐðEeFfGgHhIiJjKkLlMmNnOoPpRrSsTtUuVvXxYyÞþÄäÖöÁáÝýÚúÍíÉéÓóÆæÐðÞþ', 'NLS_SORT = ICELANDIC_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbDdÐðEeFfGgHhIiJjKkLlMmNnOoPpRrSsTtUuVvXxYyÞþÄäÖöÁáÝýÚúÍíÉéÓóÆæÐðÞþ', 'NLS_SORT = ICELANDIC_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbDdÐðEeFfGgHhIiJjKkLlMmNnOoPpRrSsTtUuVvXxYyÞþÄäÖöÁáÝýÚúÍíÉéÓóÆæÐðÞþ', 'NLS_SORT = ICELANDIC_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbDdÐðEeFfGgHhIiJjKkLlMmNnOoPpRrSsTtUuVvXxYyÞþÄäÖöÁáÝýÚúÍíÉéÓóÆæÐðÞþ', 'NLS_SORT = ICELANDIC_AI') AS lower_case_string ;


--西班牙语 (Spanish)
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_AR') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = an_ES') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = ast_ES') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_BO') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_CL') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_CO') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_CR') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_CU') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_DO') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_EC') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_ES') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_GT') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_HN') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_MX') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_NI') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_PA') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_PE') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_PR') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_SV') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_US') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_UY') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = es_VE') AS lower_case_string;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = estonian') AS lower_case_string;


SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = SPANISH') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = SPANISH_M') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = XSPANISH') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = SPANISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = SPANISH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = XSPANISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = XSPANISH_AI') AS lower_case_string ;
--意大利语 (Italian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = ca_IT') AS lower_case_string ;


SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = ITALIAN') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = ITALIAN_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = XITALIAN') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = ITALIAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = ITALIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = XITALIAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = XITALIAN_AI') AS lower_case_string ;
--葡萄牙语 (Portuguese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = PORTUGUESS') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = PORTUGUESS_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = XPORTUGUESS') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = PORTUGUESS_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = PORTUGUESS_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = XPORTUGUESS_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = XPORTUGUESS_AI') AS lower_case_string ;
--荷兰语 (Dutch)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = DUTCH') AS lower_case_string;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = DUTCH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = DUTCH_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = XDUTCH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = DUTCH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = DUTCH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = XDUTCH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = XDUTCH_AI') AS lower_case_string ;
--瑞典语 (Swedish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = SWEDISH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = SWEDISH_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = XSWEDISH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = SWEDISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = SWEDISH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = SWEDISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = SWEDISH_AI') AS lower_case_string ;
--丹麦语 (Danish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = da_DK') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = en_DK') AS lower_case_string ;


SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = DANISH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = DANISH_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = XDANISH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = DANISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = DANISH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = DANISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = DANISH_AI') AS lower_case_string ;
--挪威语 (Norwegian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = NORWEGIAN') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = NORWEGIAN_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = XNORWEGIAN') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = NORWEGIAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = NORWEGIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = NORWEGIAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = NORWEGIAN_AI') AS lower_case_string ;
--芬兰语 (Finnish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = FINNISH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = FINNISH_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = XFINNISH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = FINNISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = FINNISH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = FINNISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = FINNISH_AI') AS lower_case_string ;
--波兰语 (Polish)
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = csb_PL') AS lower_case_string ;


SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = POLISH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = POLISH_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = XPOLISH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = POLISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = POLISH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = POLISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = POLISH_AI') AS lower_case_string ;
--捷克语 (Czech)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = cs_CZ') AS lower_case_string;


SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = CZECH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = CZECH_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = XCZECH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = CZECH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = CZECH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = CZECH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = CZECH_CI') AS lower_case_string ;

--匈牙利语 (Hungarian)
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = HUNGARIAN') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = HUNGARIAN_M') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = XHUNGARIAN') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = HUNGARIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = HUNGARIAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = HUNGARIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = HUNGARIAN_CI') AS lower_case_string ;
--罗马尼亚语 (Romanian)
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = ROMANIAN') AS lower_case_string ;
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = ROMANIAN_M') AS lower_case_string ;
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = XROMANIAN') AS lower_case_string ;
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = ROMANIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = ROMANIAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = ROMANIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = ROMANIAN_CI') AS lower_case_string ;
--希腊语 (Greek)
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = el_CY') AS lower_case_string ;
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = el_GR') AS lower_case_string ;


SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = GREEK') AS lower_case_string ;
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = GREEK_M') AS lower_case_string ;
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = XGREEK') AS lower_case_string ;
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = GREEK_AI') AS lower_case_string ;
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = GREEK_CI') AS lower_case_string ;
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = GREEK_AI') AS lower_case_string ;
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = GREEK_CI') AS lower_case_string ;
--保加利亚语 (Bulgarian)
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = bg_BG') AS lower_case_string;


SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BULGARIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BULGARIAN_M') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = XBULGARIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BULGARIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BULGARIAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BULGARIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BULGARIAN_CI') AS lower_case_string ;
--乌克兰语 (Ukrainian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = crh_UA') AS lower_case_string ;


SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = UKRAINIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = UKRAINIAN_M') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = XUKRAINIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = UKRAINIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = UKRAINIAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = UKRAINIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = UKRAINIAN_CI') AS lower_case_string ;
--塞尔维亚语 (Serbian)
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = SERBIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = SERBIAN_M') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = XSERBIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = SERBIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = SERBIAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = SERBIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = SERBIAN_CI') AS lower_case_string ;
--马其顿语 (Macedonian)
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = MACEDONIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = MACEDONIAN_M') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = XMACEDONIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = MACEDONIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = MACEDONIAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = MACEDONIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = MACEDONIAN_CI') AS lower_case_string ;
--白俄罗斯语 (Belarusian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = be_BY') AS lower_case_string ;

SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BELARUSIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BELARUSIAN_M') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = XBELARUSIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BELARUSIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BELARUSIAN_CI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BELARUSIAN_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BELARUSIAN_CI') AS lower_case_string ;
--土耳其语 (Turkish)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = tr_CY') AS lower_case_string;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = tr_TR') AS lower_case_string;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = TURKISH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = TURKISH_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = XTURKISH') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = TURKISH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = TURKISH_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = TURKISH_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = TURKISH_CI') AS lower_case_string ;
--阿塞拜疆语 (Azerbaijani)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = az_AZ') AS lower_case_string;


SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = AZERBAIJANI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = AZERBAIJANI_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = XAZERBAIJANI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = AZERBAIJANI_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = AZERBAIJANI_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = AZERBAIJANI_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = AZERBAIJANI_CI') AS lower_case_string ;
--哈萨克语 (Kazakh)
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = KAZAKH') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = KAZAKH_M') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = KAZAKH') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = KAZAKH_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = KAZAKH_CI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = KAZAKH_AI') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = KAZAKH_CI') AS lower_case_string ;
--汉语 (Chinese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = SCHINESE_PINYIN_M') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = SCHINESE_PINYIN') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = SCHINESE') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = SCHINESE_PINYIN_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = SCHINESE_PINYIN_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XSCHINESE_PINYIN_CI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XSCHINESE_PINYIN_AI') AS lower_case_string ;
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XSCHINESE') AS lower_case_string ;


--通用 (BINARY)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY') AS lower_case_string ;
--英语 (English)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY') AS lower_case_string ;
--美国(American)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY') AS lower_case_string ;
--德语 (German)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = BINARY') AS lower_case_string ;
--法语 (French)
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = BINARY') AS lower_case_string ;
--西班牙语 (Spanish)
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = BINARY') AS lower_case_string ;
--意大利语 (Italian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = BINARY') AS lower_case_string ;
--葡萄牙语 (Portuguese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = BINARY') AS lower_case_string ;
--荷兰语 (Dutch)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = BINARY') AS lower_case_string ;
--瑞典语 (Swedish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = BINARY') AS lower_case_string ;
--丹麦语 (Danish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = BINARY') AS lower_case_string ;
--挪威语 (Norwegian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = BINARY') AS lower_case_string ;
--芬兰语 (Finnish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = BINARY') AS lower_case_string ;
--波兰语 (Polish)
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = BINARY') AS lower_case_string ;
--捷克语 (Czech)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = BINARY') AS lower_case_string ;
--匈牙利语 (Hungarian)
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = BINARY') AS lower_case_string ;
--罗马尼亚语 (Romanian)
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = BINARY') AS lower_case_string ;
--希腊语 (Greek)
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = BINARY') AS lower_case_string ;
--保加利亚语 (Bulgarian)
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BINARY') AS lower_case_string ;
--乌克兰语 (Ukrainian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BINARY') AS lower_case_string ;
--塞尔维亚语 (Serbian)
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = BINARY') AS lower_case_string ;
--马其顿语 (Macedonian)
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = BINARY') AS lower_case_string ;
--白俄罗斯语 (Belarusian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BINARY') AS lower_case_string ;
--土耳其语 (Turkish)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = BINARY') AS lower_case_string ;
--阿塞拜疆语 (Azerbaijani)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = BINARY') AS lower_case_string ;
--哈萨克语 (Kazakh)
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = BINARY') AS lower_case_string ;
--汉语 (Chinese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY') AS lower_case_string ;





--通用 (BINARY)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--英语 (English)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--美国(American)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--德语 (German)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--法语 (French)
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--西班牙语 (Spanish)
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--意大利语 (Italian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--葡萄牙语 (Portuguese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--荷兰语 (Dutch)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--瑞典语 (Swedish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--丹麦语 (Danish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--挪威语 (Norwegian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--芬兰语 (Finnish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--波兰语 (Polish)
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--捷克语 (Czech)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--匈牙利语 (Hungarian)
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--罗马尼亚语 (Romanian)
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--希腊语 (Greek)
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--保加利亚语 (Bulgarian)
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--乌克兰语 (Ukrainian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--塞尔维亚语 (Serbian)
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--马其顿语 (Macedonian)
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--白俄罗斯语 (Belarusian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--土耳其语 (Turkish)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--阿塞拜疆语 (Azerbaijani)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--哈萨克语 (Kazakh)
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = BINARY_M') AS lower_case_string ;
--汉语 (Chinese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_M') AS lower_case_string ;


--通用 (BINARY)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XBINARY') AS lower_case_string ;
--英语 (English)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XBINARY') AS lower_case_string ;
--美国(American)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XBINARY') AS lower_case_string ;
--德语 (German)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = XBINARY') AS lower_case_string ;
--法语 (French)
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = XBINARY') AS lower_case_string ;
--西班牙语 (Spanish)
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = XBINARY') AS lower_case_string ;
--意大利语 (Italian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = BINARY') AS lower_case_string ;
--葡萄牙语 (Portuguese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = XBINARY') AS lower_case_string ;
--荷兰语 (Dutch)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = XBINARY') AS lower_case_string ;
--瑞典语 (Swedish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = XBINARY') AS lower_case_string ;
--丹麦语 (Danish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = XBINARY') AS lower_case_string ;
--挪威语 (Norwegian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = XBINARY') AS lower_case_string ;
--芬兰语 (Finnish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = XBINARY') AS lower_case_string ;
--波兰语 (Polish)
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = XBINARY') AS lower_case_string ;
--捷克语 (Czech)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = XBINARY') AS lower_case_string ;
--匈牙利语 (Hungarian)
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = XBINARY') AS lower_case_string ;
--罗马尼亚语 (Romanian)
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = XBINARY') AS lower_case_string ;
--希腊语 (Greek)
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = XBINARY') AS lower_case_string ;
--保加利亚语 (Bulgarian)
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = XBINARY') AS lower_case_string ;
--乌克兰语 (Ukrainian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = XBINARY') AS lower_case_string ;
--塞尔维亚语 (Serbian)
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = XBINARY') AS lower_case_string ;
--马其顿语 (Macedonian)
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = XBINARY') AS lower_case_string ;
--白俄罗斯语 (Belarusian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = XBINARY') AS lower_case_string ;
--土耳其语 (Turkish)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = XBINARY') AS lower_case_string ;
--阿塞拜疆语 (Azerbaijani)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = XBINARY') AS lower_case_string ;
--哈萨克语 (Kazakh)
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = XBINARY') AS lower_case_string ;
--汉语 (Chinese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = XBINARY') AS lower_case_string ;





--通用 (BINARY)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--英语 (English)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--美国(American)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--德语 (German)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--法语 (French)
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--西班牙语 (Spanish)
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--意大利语 (Italian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--葡萄牙语 (Portuguese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--荷兰语 (Dutch)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--瑞典语 (Swedish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--丹麦语 (Danish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--挪威语 (Norwegian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--芬兰语 (Finnish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--波兰语 (Polish)
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--捷克语 (Czech)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--匈牙利语 (Hungarian)
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--罗马尼亚语 (Romanian)
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--希腊语 (Greek)
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--保加利亚语 (Bulgarian)
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--乌克兰语 (Ukrainian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--塞尔维亚语 (Serbian)
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--马其顿语 (Macedonian)
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--白俄罗斯语 (Belarusian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--土耳其语 (Turkish)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--阿塞拜疆语 (Azerbaijani)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--哈萨克语 (Kazakh)
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = BINARY_AI') AS lower_case_string ;
--汉语 (Chinese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_AI') AS lower_case_string ;



--通用 (BINARY)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--英语 (English)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--美国(American)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--德语 (German)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÄäÖöÜüẞß', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--法语 (French)
SELECT NLS_LOWER('AaBbCcDdEeÉéÈèÊêËëFfGgHhIiÎîÏïJjKkLlMmNnOoÔôÖöPpQqRrSsTtUuÛûÜüVvWwXxYyŸÿZz', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--西班牙语 (Spanish)
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--意大利语 (Italian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÈèÉéÌìÒòÓóÙù', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--葡萄牙语 (Portuguese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÂâÃãÄäÈèÉéÊêËëÌìÍíÎîÏïÒòÓóÔôÕõÖöÙùÚúÛûÜüÇç', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--荷兰语 (Dutch)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÀàÁáÇçÉéËëÍíÏïÓóÖöÚúÜüÝý', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--瑞典语 (Swedish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--丹麦语 (Danish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--挪威语 (Norwegian)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÆæØøÅå', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--芬兰语 (Finnish)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZzÅåÄäÖö', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--波兰语 (Polish)
SELECT NLS_LOWER('AaBbCcĆćDdEeĘęFfGgHhIiJjKkLlŁłMmNnŃńOoÓóPpRrSsŚśTtUuWwYyZzŹźŻż', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--捷克语 (Czech)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhChchIiJjKkLlMmNnOoPpQqRrSsŠšTtŤťUuVvWwXxYyZzŽž', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--匈牙利语 (Hungarian)
SELECT NLS_LOWER('AaÁáBbCcCscsDdDzdzDzdzsEeÉéFfGg GygyHhIiÍíJjKkLlLylyMmNnNynyOoÓóÖöŐőPpQqRrSsSzszTtTytyUuÚúÜüŰűVvWwXxYyZzZszs', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--罗马尼亚语 (Romanian)
SELECT NLS_LOWER('AaĂăÂâBbCcCiçıDdEeFfGgGiğıHhIiÎîJjKkLlMmNnOoPpQqRrSsȘșTtȚțUuVvWwXxYyZz', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--希腊语 (Greek)
SELECT NLS_LOWER('ΑαΒβΓγΔδΕεΖζΗηΘθΙιΚκΛλΜμΝνΞξΟοΠπΡρΣσςΤτΥυΦφΧχΨψΩω', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--保加利亚语 (Bulgarian)
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--乌克兰语 (Ukrainian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЄєЖжЗзИиІіЇїЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--塞尔维亚语 (Serbian)
SELECT NLS_LOWER('АаБбВвГгДдЂђЕеЖ⚗ИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЋћУуФфХхЦцЧчЏџШш', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--马其顿语 (Macedonian)
SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--白俄罗斯语 (Belarusian)
SELECT NLS_LOWER('АаБбВвГгҐґДдЕеЁёЖжЗзІіЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЬьЮюЯя', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--土耳其语 (Turkish)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhIıİiJjKkLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--阿塞拜疆语 (Azerbaijani)
SELECT NLS_LOWER('AaBbCcÇçDdEeFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--哈萨克语 (Kazakh)
SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = BINARY_CI') AS lower_case_string ;
--汉语 (Chinese)
SELECT NLS_LOWER('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 'NLS_SORT = BINARY_CI') AS lower_case_string ;




--语言不对应
SELECT NLS_LOWER('AaÁáÀàÂâÄäBbCcDdEeÉéÈèÊêËëFfGgHhIiÍíÌìÎîÏïJjKkLlMmNnÑñOoÓóÒòÔôÖöPpQqRrSsTtUuÚúÙùÛûÜüVvWwXxYyÝýZz', 'NLS_SORT = az_AZ') AS lower_case_string ;

SELECT NLS_LOWER('АаБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоПпРрСсТтУуҰұФфХхҺһЦцЧчШшЩщЪъЫыЬьЭэЮюЯя', 'NLS_SORT = az_AZ') AS lower_case_string ;

SELECT NLS_LOWER('АаБбВвГгДдЃѓЕеЖжЗзЅѕИиЈјКкЛлЉљМмНнЊ⚗ОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш', 'NLS_SORT = az_AZ') AS lower_case_string ;



--检验clob类型
------------------------------------------------------------------------------------------------------------
SELECT NLS_LOWER(cast('AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz' AS CLOB), 'NLS_SORT = BINARY') AS lower_case_string ;

drop table example_clob;

CREATE TABLE example_clob (
                               id          SERIAL PRIMARY KEY, -- 自动递增的主键
                               name        VARCHAR2(100),                                       -- 普通字符串列
                               clob_column CLOB
);
INSERT INTO example_clob (name, clob_column)
VALUES ('Example Entry', TO_CLOB('This is a large text entry that will be stored in the CLOB column.'));


--检验nls_lower中嵌套cast转clob能否成功
SELECT NLS_LOWER(clob_column) FROM example_clob;



-----------------------------------------------------------------------------------------------------------------------
--oracle
--嵌套表类型,集合类型
-- 定义 VARRAY 类型
CREATE TYPE number_varray_type AS VARRAY(10) OF NUMBER;


-- 创建表时使用 VARRAY 类型
CREATE TABLE employee_skills (
                                 employee_id NUMBER PRIMARY KEY,
                                 skills      number_varray_type
);

-- 定义嵌套表类型
CREATE TYPE string_table_type AS TABLE OF VARCHAR2(100);


-- 创建表时使用嵌套表类型
CREATE TABLE project_tasks (
                               project_id NUMBER PRIMARY KEY,
                               tasks      string_table_type
) NESTED TABLE tasks STORE AS tasks_nt;


INSERT INTO employee_skills (employee_id, skills)
VALUES (1, number_varray_type(101, 102, 103));

INSERT INTO project_tasks (project_id, tasks)
VALUES (1, string_table_type('Task 1', 'Task 2', 'Task 3'));

select * from project_tasks;

select nls_lower(tasks) from project_tasks;

--opengauss
-- 创建一个包含数组列的表
drop table employee_skills;
CREATE TABLE employee_skills (
                                 employee_id INT PRIMARY KEY,
                                 skills      NUMERIC[] ,-- 使用 PostgreSQL/OpenGauss 数组类型
                                 strings      varchar(20)[]
);

-- 插入数据到带有数组列的表中
INSERT INTO employee_skills (employee_id, skills,strings)
VALUES (1, ARRAY[101, 102, 103],ARRAY['WEFWEF', 'WEFHTHT', 'WIEJFLW']);

select * from employee_skills;
------------------------------------------------------------------------------------------------------------------------
--oracle
--创建一张student表
drop table student;
CREATE TABLE student (
                         student_id INT PRIMARY KEY,-- 自动递增的学生ID
                         first_name VARCHAR2(50) NOT NULL, -- 名字
                         last_name VARCHAR2(50) NOT NULL, -- 姓氏
                         date_of_birth DATE NOT NULL, -- 出生日期
                         gender CHAR(1), -- 性别，M 或 F
                         email VARCHAR2(100) UNIQUE -- 电子邮件，唯一约束
);

-- 插入单个学生记录
INSERT INTO student (student_id,first_name, last_name, date_of_birth, gender, email)
VALUES (1,'张', '三', TO_DATE('1995-07-23', 'YYYY-MM-DD'), 'M', 'zhangsan@example.com');

-- 插入单个学生记录
INSERT INTO student (student_id,first_name, last_name, date_of_birth, gender, email)
VALUES (2,'李', '四', TO_DATE('1994-05-15', 'YYYY-MM-DD'), 'F', 'lisi@example.com');

-- 插入单个学生记录
INSERT INTO student (student_id,first_name, last_name, date_of_birth, gender, email)
VALUES (3,'王', '五', TO_DATE('1996-12-01', 'YYYY-MM-DD'), 'M', 'wangwu@example.com');

-- 插入单个学生记录
INSERT INTO student (student_id,first_name, last_name, date_of_birth, gender, email)
VALUES (4,'赵', '六', TO_DATE('1997-03-18', 'YYYY-MM-DD'), 'F', 'zhaoliu@example.com');

select * from student;

--nls_lower中包含子查询
select nls_lower((select gender from student)) ;

select gender from student;

-----------------------------------------------------------------------------
--opengauss
--创建一张student表
drop table student;
CREATE TABLE student (
                         student_id SERIAL PRIMARY KEY,-- 自动递增的学生ID
                         first_name VARCHAR2(50) NOT NULL, -- 名字
                         last_name VARCHAR2(50) NOT NULL, -- 姓氏
                         date_of_birth DATE NOT NULL, -- 出生日期
                         gender CHAR(1), -- 性别，M 或 F
                         email VARCHAR2(100) UNIQUE -- 电子邮件，唯一约束
);

-- 插入单个学生记录
INSERT INTO student (first_name, last_name, date_of_birth, gender, email)
VALUES ('张', '三', TO_DATE('1995-07-23', 'YYYY-MM-DD'), 'M', 'zhangsan@example.com');

-- 插入单个学生记录
INSERT INTO student (first_name, last_name, date_of_birth, gender, email)
VALUES ('李', '四', TO_DATE('1994-05-15', 'YYYY-MM-DD'), 'F', 'lisi@example.com');

-- 插入单个学生记录
INSERT INTO student (first_name, last_name, date_of_birth, gender, email)
VALUES ('王', '五', TO_DATE('1996-12-01', 'YYYY-MM-DD'), 'M', 'wangwu@example.com');

-- 插入单个学生记录
INSERT INTO student (first_name, last_name, date_of_birth, gender, email)
VALUES ('赵', '六', TO_DATE('1997-03-18', 'YYYY-MM-DD'), 'F', 'zhaoliu@example.com');

select * from student;

--nls_lower中包含子查询
select nls_lower((select gender from student)) ;

select gender from student;


--oracle
--被包含
select to_number(nls_lower('ASDAIOJOmalksJOW') default 999 on conversion error );

select to_number(123 default nls_lower('123') on conversion error );

select to_char(nls_lower('ASDAIOJOmalksJOW') );

select to_char(nls_lower(gender) )from student;



--opengauss
--被包含
select to_number(nls_lower('ASDAIOJOmalksJOW') default 999 on conversion error );

select to_number(123 default nls_lower('123') on conversion error );

select to_char(nls_lower('ASDAIOJOmalksJOW') );

select to_char(nls_lower(gender) )from student;


-----------------------------------------------------------------------------------------------------------------

--数据类型
--用户可以想到的合法输入
--测试列
select nls_lower(gender) from student;

--nls_lower()函数可以想到的合法数据类型
drop table student;
CREATE TABLE student (
                         student_id SERIAL PRIMARY KEY, -- 自动递增的学生ID
                         first_name VARCHAR2(50) NOT NULL, -- 名字
                         last_name VARCHAR2(50) NOT NULL, -- 姓氏
                         date_of_birth DATE NOT NULL, -- 出生日期
                         gender CHAR(1), -- 性别，M 或 F
                         email VARCHAR2(100) UNIQUE -- 电子邮件，唯一约束
);

-- 插入单个学生记录
INSERT INTO student (first_name, last_name, date_of_birth, gender, email)
VALUES ('张', '三', TO_DATE('1995-07-23', 'YYYY-MM-DD'), 'M', 'zhangsan@example.com');

-- 插入单个学生记录
INSERT INTO student (first_name, last_name, date_of_birth, gender, email)
VALUES ('李', '四', TO_DATE('1994-05-15', 'YYYY-MM-DD'), 'F', 'lisi@example.com');

-- 插入单个学生记录
INSERT INTO student (first_name, last_name, date_of_birth, gender, email)
VALUES ('王', '五', TO_DATE('1996-12-01', 'YYYY-MM-DD'), 'M', 'wangwu@example.com');

-- 插入单个学生记录
INSERT INTO student (first_name, last_name, date_of_birth, gender, email)
VALUES ('赵', '六', TO_DATE('1997-03-18', 'YYYY-MM-DD'), 'F', 'zhaoliu@example.com');

commit;
--列
select * from student;

select nls_lower(cast(gender as varchar(20)))from student;

select nls_lower(cast(gender as nvarchar(20)))from student;

select nls_lower(cast(gender as TEXT))from student;

select nls_lower(cast(gender as clob))from student;

select nls_lower(cast(gender as varchar2(20)))from student;

select nls_lower(cast(gender as nvarchar2(20)))from student;

select nls_lower(cast(gender as nchar(20)))from student;

select nls_lower(cast(gender as char(20)))from student;

select nls_lower(cast(gender as CHARACTER(20)))from student;

--直接转换
select nls_lower(cast('ascaSDSV' as varchar(20)));

select nls_lower(cast('ascaSDSV' as nvarchar(20)));

select nls_lower(cast('ascaSDSV' as TEXT));

select nls_lower(cast('ascaSDSV' as clob));

select nls_lower(cast('ascaSDSV' as varchar2(20)));

select nls_lower(cast('ascaSDSV' as nvarchar2(20)));

select nls_lower(cast('ascaSDSV' as nchar(20)));

select nls_lower(cast('ascaSDSV' as char(20)));

select nls_lower(cast('ascaSDSV' as CHARACTER(20)));


--::
select nls_lower(cast(gender :: varchar(20)as varchar(20)))from student;

select nls_lower(cast(gender :: nvarchar(20)as varchar(20)))from student;

select nls_lower(cast(gender :: TEXT)as varchar(20))from student;

select nls_lower(cast(gender :: clob)as varchar(20))from student;

select nls_lower(cast(gender :: varchar2(20)as varchar(20)))from student;

select nls_lower(cast(gender :: nvarchar2(20)as varchar(20)))from student;

select nls_lower(cast(gender :: nchar(20)as varchar(20)))from student;

select nls_lower(cast(gender :: char(20)as varchar(20)))from student;

select nls_lower(cast(gender :: CHARACTER(20)as varchar(20)))from student;

--直接转换
select nls_lower(cast('ascaSDSV' :: varchar(20)as TEXT));

select nls_lower(cast('ascaSDSV' :: nvarchar(20)as TEXT));

select nls_lower(cast('ascaSDSV' :: TEXT as TEXT));

select nls_lower(cast('ascaSDSV' :: clob as TEXT));

select nls_lower(cast('ascaSDSV' :: varchar2(20) as TEXT));

select nls_lower(cast('ascaSDSV' :: nvarchar2(20) as TEXT));

select nls_lower(cast('ascaSDSV' :: nchar(20) as TEXT));

select nls_lower(cast('ascaSDSV' :: char(20) as TEXT));

select nls_lower(cast('ascaSDSV' :: CHARACTER(20) as TEXT));
--不合法
--空
select nls_lower('');
select nls_lower(''+'');
select nls_lower(null);
select nls_lower(' ');

--不合法的输入类型


------------------------------------------------------------------------------------------------------------------------
--oracle
--输入格式
select nls_lower(123+'ASEFWEF');
select nls_lower(123+'123');
select nls_lower(to_number(1231)+'123');
select nls_lower(123||'ASEFWEF');
select nls_lower('qnoPWOEPWEJFPWJEPF'||'ASEFWEF');
select nls_lower('  ASEFWEF ');
select nls_lower('  AS EF WE F ');
select nls_lower(NULL+'  ASEFWEF ');
select nls_lower('  ASE123897FW(*&(中文字符EF ');

------------------------------------------------------------------------------------------------------------------------
--opengauss
--输入格式
select nls_lower(123+'ASEFWEF');
select nls_lower(123+'123');
select nls_lower(to_number(1231)+'123');
select nls_lower(123||'ASEFWEF');
select nls_lower('qnoPWOEPWEJFPWJEPF'||'ASEFWEF');
select nls_lower('  ASEFWEF ');
select nls_lower('  AS EF WE F ');
select nls_lower(NULL+'  ASEFWEF ');
select nls_lower('  ASE123897FW(*&(中文字符EF ');

--oracle
--不合法输入格式
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BULGARIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BUL按时打卡AN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_拉克丝的 = BULGARIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS卢卡斯RT = BUL阿斯顿IAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NL S_ SORT = BUL GA RIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', '    NLS_SORT = BULGARIAN  ') AS lower_case_string ;

--opengauss
--不合法输入格式
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BULGARIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_SORT = BUL按时打卡AN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS_拉克丝的 = BULGARIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NLS卢卡斯RT = BUL阿斯顿IAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', 'NL S_ SORT = BUL GA RIAN') AS lower_case_string ;
SELECT NLS_LOWER('АаБбВвГгДдЕеЖжЗзИиЙйКкЛлМмНнОоПпРрСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЮюЯя', '    NLS_SORT = BULGARIAN  ') AS lower_case_string ;

--create
drop table student;
CREATE TABLE student (
                         student_id SERIAL PRIMARY KEY,-- 自动递增的学生ID
                         first_name VARCHAR2(50) NOT NULL, -- 名字
                         last_name VARCHAR2(50) NOT NULL, -- 姓氏
                         date_of_birth DATE NOT NULL, -- 出生日期
                         gender CHAR(1), -- 性别，M 或 F
                         email VARCHAR2(100) UNIQUE -- 电子邮件，唯一约束
);


drop table student;
CREATE TABLE student (
                         student_id SERIAL PRIMARY KEY, -- 自动递增的学生ID
                         first_name VARCHAR2(50) NOT NULL, -- 名字
                         last_name VARCHAR2(50) NOT NULL, -- 姓氏
                         date_of_birth DATE NOT NULL, -- 出生日期
                         gender CHAR(1), -- 性别，M 或 F
                         email VARCHAR2(100) UNIQUE, -- 电子邮件，唯一约束
                         lower nls_lower(123)
);


--create as
drop table student1;
CREATE TABLE student1 AS
SELECT nls_lower(gender) lower_gender FROM student;

select * from student1;

drop table student1;
CREATE TABLE nls_lower('STUDENT1') AS
SELECT nls_lower(gender) lower_gender FROM student;

--alter
drop table student;
CREATE TABLE student (
                         student_id SERIAL PRIMARY KEY,-- 自动递增的学生ID
                         first_name VARCHAR2(50) NOT NULL, -- 名字
                         last_name VARCHAR2(50) NOT NULL, -- 姓氏
                         date_of_birth DATE NOT NULL, -- 出生日期
                         gender CHAR(1), -- 性别，M 或 F
                         email VARCHAR2(100) UNIQUE -- 电子邮件，唯一约束
);
ALTER TABLE student ADD (email1 VARCHAR2(100));

ALTER TABLE student ADD (email1 VARCHAR2(100) default nls_lower('email1'));

--drop
drop table nls_lower('TESTDROP');

--truncate(q清空表中的所有内容)
TRUNCATE TABLE nls_lower('STUDENT');

--merge into演示
--功能使用
drop table employees;
CREATE TABLE employees (
                           employee_id NUMBER PRIMARY KEY,
                           first_name VARCHAR2(50),
                           last_name VARCHAR2(50),
                           salary NUMBER
);

INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 60000);
INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (2, 'Jane', 'Smith', 70000);
COMMIT;
drop table new_employees;
CREATE TABLE new_employees (
                               employee_id NUMBER PRIMARY KEY,
                               first_name VARCHAR2(50),
                               last_name VARCHAR2(50),
                               salary NUMBER
);

INSERT INTO new_employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 65000); -- 更新现有员工的薪水
INSERT INTO new_employees (employee_id, first_name, last_name, salary) VALUES (3, 'Alice', 'Johnson', 80000); -- 新增员工
COMMIT;

MERGE INTO employees e
USING new_employees ne
ON (e.employee_id = ne.employee_id)
WHEN MATCHED THEN
    UPDATE SET e.salary = ne.salary -- 只更新薪水字段
WHEN NOT MATCHED THEN
    INSERT (employee_id, first_name, last_name, salary)
    VALUES (ne.employee_id, ne.first_name, ne.last_name, ne.salary);

SELECT * FROM employees;
--nls_lower
drop table employees;
drop table new_employees;

CREATE TABLE employees (
                           employee_id NUMBER PRIMARY KEY,
                           first_name VARCHAR2(50),
                           last_name VARCHAR2(50),
                           salary NUMBER
);

INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 60000);
INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (2, 'Jane', 'Smith', 70000);
COMMIT;

CREATE TABLE new_employees (
                               employee_id NUMBER PRIMARY KEY,
                               first_name VARCHAR2(50),
                               last_name VARCHAR2(50),
                               salary NUMBER
);

INSERT INTO new_employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 65000); -- 更新现有员工的薪水
INSERT INTO new_employees (employee_id, first_name, last_name, salary) VALUES (3, 'Alice', 'Johnson', 80000); -- 新增员工
COMMIT;

--on处使用函数
MERGE INTO employees e
USING new_employees ne
ON (e.employee_id = nls_lower(ne.employee_id))
WHEN MATCHED THEN
    UPDATE SET e.salary = ne.salary -- 只更新薪水字段
WHEN NOT MATCHED THEN
    INSERT (employee_id, first_name, last_name, salary)
    VALUES (ne.employee_id, ne.first_name, ne.last_name, ne.salary);

select * from employees;

--insert
drop table employees;

CREATE TABLE employees (
                           employee_id NUMBER PRIMARY KEY,
                           first_name VARCHAR2(50),
                           last_name VARCHAR2(50),
                           salary NUMBER
);


INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', nls_lower('WANG Cires'), 60000);
COMMIT;

select * from employees;

--update
drop table employees;
CREATE TABLE employees (
                           employee_id NUMBER PRIMARY KEY,
                           first_name VARCHAR2(50),
                           last_name VARCHAR2(50),
                           salary NUMBER
);


INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', nls_lower('WANG Cires'), 60000);
COMMIT;

select * from employees;

UPDATE employees
SET salary = 70000
WHERE employee_id = nls_lower(1);

select * from employees;

--delete
drop table employees;
CREATE TABLE employees (
                           employee_id NUMBER PRIMARY KEY,
                           first_name VARCHAR2(50),
                           last_name VARCHAR2(50),
                           salary NUMBER
);


INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', nls_lower('WANG Cires'), 60000);
COMMIT;

select * from employees;
DELETE FROM employees
WHERE employee_id = 1;
select * from employees;
--union
drop table employees;
CREATE TABLE employees (
                           employee_id NUMBER PRIMARY KEY,
                           first_name VARCHAR2(50),
                           last_name VARCHAR2(50),
                           salary NUMBER
);

INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 60000);
INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (2, 'Jane', 'Smith', 70000);
COMMIT;
drop table new_employees;
CREATE TABLE new_employees (
                               employee_id NUMBER PRIMARY KEY,
                               first_name VARCHAR2(50),
                               last_name VARCHAR2(50),
                               salary NUMBER
);

INSERT INTO new_employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 65000); -- 更新现有员工的薪水
INSERT INTO new_employees (employee_id, first_name, last_name, salary) VALUES (3, 'Alice', 'Johnson', 80000); -- 新增员工
COMMIT;

select * from employees where employee_id=1
union
select * from new_employees;



--join
drop table employees;
CREATE TABLE employees (
                           employee_id NUMBER PRIMARY KEY,
                           first_name VARCHAR2(50),
                           last_name VARCHAR2(50),
                           salary NUMBER
);

INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 60000);
INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (2, 'Jane', 'Smith', 70000);
COMMIT;
drop table new_employees;
CREATE TABLE new_employees (
                               employee_id NUMBER PRIMARY KEY,
                               first_name VARCHAR2(50),
                               last_name VARCHAR2(50),
                               salary NUMBER
);

INSERT INTO new_employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 65000); -- 更新现有员工的薪水
INSERT INTO new_employees (employee_id, first_name, last_name, salary) VALUES (3, 'Alice', 'Johnson', 80000); -- 新增员工
COMMIT;
select * from employees;

select * from new_employees;

select * from employees
left join
new_employees
on employees.employee_id=new_employees.employee_id
WHERE employees.first_name=('J'||NLS_LOWER('OHN'));


--distinct
drop table employees;
CREATE TABLE employees (
                           employee_id NUMBER PRIMARY KEY,
                           first_name VARCHAR2(50),
                           last_name VARCHAR2(50),
                           salary NUMBER
);

INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 60000);
INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (2, 'Jane', 'Smith', 70000);
COMMIT;
drop table new_employees;
CREATE TABLE new_employees (
                               employee_id NUMBER PRIMARY KEY,
                               first_name VARCHAR2(50),
                               last_name VARCHAR2(50),
                               salary NUMBER
);

INSERT INTO new_employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 65000); -- 更新现有员工的薪水
INSERT INTO new_employees (employee_id, first_name, last_name, salary) VALUES (3, 'Alice', 'Johnson', 80000); -- 新增员工
COMMIT;
select * from employees;

select * from new_employees;

select distinct * from employees
                  left join
              new_employees
              on employees.employee_id=new_employees.employee_id
WHERE employees.first_name=('J'||NLS_LOWER('OHN'));

--group by ... having
drop table employees;
CREATE TABLE employees (
                           employee_id NUMBER PRIMARY KEY,
                           first_name VARCHAR2(50),
                           last_name VARCHAR2(50),
                           salary NUMBER
);

INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 60000);
INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (2, 'Jane', 'Smith', 70000);
COMMIT;
select sum(employee_id),nls_lower(first_name) from employees group by employee_id,first_name having nls_lower(employee_id)=1;

--order by
drop table employees;
CREATE TABLE employees (
                           employee_id NUMBER PRIMARY KEY,
                           first_name VARCHAR2(50),
                           last_name VARCHAR2(50),
                           salary NUMBER
);

INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (1, 'John', 'Doe', 60000);
INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (2, 'Jane', 'Smith', 70000);
COMMIT;

select *
from employees
order by nls_lower(first_name);


--分区表
--范围分区
drop table sales;
CREATE TABLE sales (
                       sale_id NUMBER,
                       sale_date DATE,
                       customer_id NUMBER,
                       strtest varchar(20)
                   )
    PARTITION BY RANGE (sale_date)
(
    PARTITION p1 VALUES LESS THAN (TO_DATE('2020-01-01', 'YYYY-MM-DD')),
    PARTITION p2 VALUES LESS THAN (TO_DATE('2021-01-01', 'YYYY-MM-DD')),
    PARTITION p3 VALUES LESS THAN (TO_DATE('2022-01-01', 'YYYY-MM-DD')),
    PARTITION p4 VALUES LESS THAN (TO_DATE('2023-01-01', 'YYYY-MM-DD'))
);
insert into sales values (1,TO_DATE('2021-05-09', 'YYYY-MM-DD'),1,'ASDF');

select nls_lower(strtest) from sales;




--哈希分区
drop table customers;
CREATE TABLE customers (
                           customer_id NUMBER,
                           customer_name VARCHAR2(100)
)
    PARTITION BY HASH (customer_id)
    PARTITIONS 4;

insert into customers values (1,'ASFFG');

select nls_lower(customer_name) from customers;

--列表分区
DROP TABLE employees;
CREATE TABLE employees (
                           employee_id NUMBER,
                           department_id NUMBER,
                           employee_name VARCHAR2(100)
)
    PARTITION BY LIST (department_id)
(
    PARTITION p1 VALUES (1, 2),
    PARTITION p2 VALUES (3, 4),
    PARTITION p3 VALUES (5, 6)
);

insert into employees values (1,3,'ASDG');

select nls_lower(employee_name) from employees;


--区间分区
drop table sales;
CREATE TABLE sales (
                       sale_id   BIGINT,
                       sale_date DATE,
                       amount    NUMERIC,
                       strtest   VARCHAR(20)
)
    PARTITION BY RANGE (sale_date)
(
    PARTITION p_initial VALUES LESS THAN ('2024-01-01')
);
insert into sales values (1,to_date('2020-02-01','yyyy-mm-dd'),1,'SDFSDFS');

select nls_lower(strtest) from sales;


--二级分区
drop table sales;
CREATE TABLE sales (
                       sale_id NUMBER,
                       sale_date DATE,
                       customer_id NUMBER,
                       amount NUMBER,
                       strtest varchar2(20)

)
    PARTITION BY RANGE (sale_date)
    SUBPARTITION BY HASH (customer_id)
    SUBPARTITIONS 4 -- 每个范围分区包含 4 个哈希子分区
(
    PARTITION p2023 VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD')),
    PARTITION p2024 VALUES LESS THAN (TO_DATE('2025-01-01', 'YYYY-MM-DD'))
);

insert into sales values (1,to_date('2020-02-01','yyyy-mm-dd'),1,1,'ASDV');

select nls_lower(strtest) from sales;



-----------------------------------------
--视图
--普通视图
drop table sales;
CREATE TABLE sales (
                       sale_id NUMBER,
                       sale_date DATE,
                       customer_id NUMBER,
                       amount NUMBER,
                       strtest varchar2(20)

)
    PARTITION BY RANGE (sale_date)
    SUBPARTITION BY HASH (customer_id)
    SUBPARTITIONS 4 -- 每个范围分区包含 4 个哈希子分区
(
    PARTITION p2023 VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD')),
    PARTITION p2024 VALUES LESS THAN (TO_DATE('2025-01-01', 'YYYY-MM-DD'))
);

insert into sales values (1,to_date('2020-02-01','yyyy-mm-dd'),1,1,'ASDV');

drop view sim_sales_view;

create view sim_sales_view as
select nls_lower(strtest) strtest from sales;

select nls_lower(strtest) from sim_sales_view;

----------------------------
--物化视图
drop table sales;
CREATE TABLE sales (
                       sale_id NUMBER,
                       sale_date DATE,
                       customer_id NUMBER,
                       amount NUMBER,
                       strtest varchar2(20)

)
    PARTITION BY RANGE (sale_date)
    SUBPARTITION BY HASH (customer_id)
    SUBPARTITIONS 4 -- 每个范围分区包含 4 个哈希子分区
(
    PARTITION p2023 VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD')),
    PARTITION p2024 VALUES LESS THAN (TO_DATE('2025-01-01', 'YYYY-MM-DD'))
);

insert into sales values (1,to_date('2020-02-01','yyyy-mm-dd'),1,1,'ASDV');

drop MATERIALIZED view mat_sales_view;

create MATERIALIZED VIEW mat_sales_view as
select nls_lower(strtest) strtest from sales;

select nls_lower(strtest) from mat_sales_view;

-------------------------------
--增量物化视图(这个需要在opengauss上验证)
DROP TABLE company;
CREATE TABLE company (
                         id integer PRIMARY KEY,
                         name varchar(10)  NOT NULL,
                         age integer NOT NULL,
                         address varchar(20) NOT NULL,
                         salary integer NOT NULL
);

--插入测试数据
INSERT INTO company VALUES (1, 'Paul', 32, 'California', 20000);
INSERT INTO company VALUES (2, 'Allen', 25, 'Texas', 15000);
commit;
CREATE INCREMENTAL MATERIALIZED VIEW view_company AS
select * from company;

REFRESH INCREMENTAL MATERIALIZED VIEW view_company;

SELECT * FROM view_company;

DROP MATERIALIZED VIEW view_company;


-----------------------------------------------------------------
--作为列的默认值
drop table orders;
CREATE TABLE orders (
                        order_id SERIAL PRIMARY KEY,
                        region VARCHAR2(50) DEFAULT nls_lower('ASDF'),
                        amount NUMBER DEFAULT 0
);

insert into orders values (1,null,null);

select * from orders;

select region from orders;


---------------------------------------
--在函数中使用nls_lower
CREATE OR REPLACE FUNCTION lower_with_locale (
    input_string IN VARCHAR2,
    locale_name  IN VARCHAR2 DEFAULT 'bg_BG'
) RETURN VARCHAR2
    IS
BEGIN
    -- 使用 NLS_LOWER 函数根据指定的语言环境转换为小写
    RETURN NLS_LOWER(input_string, 'NLS_SORT=' || locale_name);
END;
/

-- 使用默认语言环境 'AMERICAN'
SELECT lower_with_locale('HELLO WORLD') AS result ;

-- 指定语言环境
SELECT lower_with_locale('HALLO WELT', 'tr_CY') AS result ;

--------------------------------------------
--创建一个存储过程
--先创建一张表存储存储过程的生成结果
drop table sp;
create table sp (
    test varchar2(20)
);

CREATE OR REPLACE PROCEDURE convert_to_lower (
    p_input_string IN VARCHAR2  -- 输入字符串
)
    IS
BEGIN
    insert into sp
    select nls_lower(p_input_string) ;
END;
/


call  convert_to_lower('ASDFRG');


SELECT * FROM SP;
----------------------------
--包（packages）
CREATE OR REPLACE PACKAGE string_operations AS
    -- 公共过程：将字符串转换为小写并插入到表中
    PROCEDURE convert_to_lower (
        p_input_string IN VARCHAR2  -- 输入字符串
    );

    -- 如果需要，还可以在这里声明其他公共函数或过程
END string_operations;
/

CREATE OR REPLACE PACKAGE BODY string_operations AS

    -- 实现公共过程
    PROCEDURE convert_to_lower (
        p_input_string IN VARCHAR2  -- 输入字符串
    ) IS
    BEGIN
        -- 插入转换后的字符串到表 sp 中
        INSERT INTO sp (test)
        SELECT NLS_LOWER(p_input_string) ;

        -- 提交事务以保存更改（可选）
        COMMIT;
    -- 如果需要，可以在此处添加私有过程或函数

BEGIN
    -- 初始化代码块（可选）
    NULL; -- 这里可以放置一些初始化逻辑
END string_operations;
/

BEGIN
    -- 调用 Package 中的过程
    string_operations.convert_to_lower('NCIUDI');
END;
/

-- 查询表中的内容
SELECT * FROM sp;

-----------------------------------------------
--触发器
drop table employees;
CREATE TABLE employees (
        employee_id NUMBER PRIMARY KEY,
        first_name VARCHAR2(50),
        last_name VARCHAR2(50),
        salary NUMBER
             );

        CREATE OR REPLACE FUNCTION lowercase_names_trigger()
RETURNS trigger AS $$
        BEGIN
            -- 将名字和姓氏转换为小写
            NEW.first_name := NLS_LOWER(NEW.first_name);
            NEW.last_name := NLS_LOWER(NEW.last_name);

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER trg_lower_case_names
BEFORE INSERT ON employees
        FOR EACH ROW
        EXECUTE PROCEDURE lowercase_names_trigger();

INSERT INTO employees (employee_id,first_name, last_name,salary) VALUES (1,'JOHN', 'DOE',123);
INSERT INTO employees (employee_id,first_name, last_name,salary) VALUES (2,'JANE', 'SMITH',234);

-- 查询表中的内容
SELECT * FROM employees;


-------------------------------------------------------------
--prepare（仅有postgressql支持）
-- 创建一个简单的表（如果尚未存在）
-- 如果表结构不匹配，先删除旧表
-- 准备一个带有条件的查询
PREPARE search_employee (TEXT) AS
SELECT * FROM employees WHERE NLS_LOWER(last_name) = NLS_LOWER($1);

-- 执行准备好的查询
EXECUTE search_employee('DOE');

-------------------------------------------
--表达式
select 'asce`32'||nls_lower('OQIWJDOQN');

SELECT TO_NUMBER('1231'+NLS_LOWER('1231'));


-----------------------------------------------
--行存表
drop table row_table;
create table row_table(
    strtest varchar2(20)
);
insert into row_table values ('ASIOJAS');

select nls_lower(strtest) from row_table;

--列存表
DROP TABLE customer_test2;
CREATE TABLE customer_test2
(
  state_ID   CHAR(2),
  state_NAME VARCHAR2(40),
  area_ID    NUMBER
)
WITH (ORIENTATION = COLUMN);

insert into customer_test2 values('JK','GEYMUMSW','1');
insert into customer_test2 values('AS','GERSCGTRTHW','1');
insert into customer_test2 values('VR','GWDQVDVSW','1');

SELECT * FROM customer_test2;

SELECT NLS_LOWER(state_ID) FROM customer_test2;



--临时表
-- 创建一个每事务结束都会清空的临时表



-- 提交事务（这将删除所有行）
COMMIT;

-- 再次查询（此时应该没有数据）
SELECT * FROM temp_sales;



---------------------------------------------------
--where中使用nls_lower函数
drop table employees;
CREATE TABLE employees (
                           employee_id NUMBER PRIMARY KEY,
                           first_name VARCHAR2(50),
                           last_name VARCHAR2(50),
                           salary NUMBER
);

insert into employees values (1,'ASDASDJAO','IASHDOIASD',1);

SELECT nls_lower(first_name) from employees where nls_lower(first_name)='asdasdjao';




--枚举类型
-- 1. 创建枚举类型
CREATE TYPE order_status AS ENUM ('Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled');

-- 2. 创建包含枚举类型列的表
DROP TABLE orders;
CREATE TABLE orders (
        order_id SERIAL PRIMARY KEY,
        customer_name VARCHAR(100) NOT NULL,
        status order_status NOT NULL DEFAULT 'Pending'
             );

-- 3. 插入订单记录
INSERT INTO orders (customer_name, status)
VALUES
    ('Alice', 'Pending'),
('Bob', 'Processing'),
('Charlie', 'Shipped'),
('David', 'Delivered'),
('Eve', 'Cancelled');

-- 4. 查询所有订单
SELECT * FROM orders;

SELECT NLS_LOWER(status) FROM orders;

-- 7. 删除枚举类型（如果不再需要）
DROP TABLE orders;
DROP TYPE order_status;


create table test1(col1 blob);

INSERT INTO test1 (col1)
VALUES ( '47'::blob);
INSERT INTO test1 (col1)
VALUES ( 'C49E'::blob);

select nls_lower(col1) from test1;

drop table TS;
CREATE TABLE TS(COL1 BYTEA, COL2 VARCHAR(20), COL3 CHAR(20), COL4 TEXT);
INSERT INTO TS VALUES('rrtbrta','qwdqac','\x220203','QPOEDQP');
INSERT INTO TS VALUES('aiojoihcASC','\x523203','\x810203','MCKSNCKSDC');
INSERT INTO TS VALUES ('\xc48c45534bc39d' , '\xc48c45534bc39d', '\xc48c45534bc39d', 'CWIVJOWV');
INSERT INTO TS VALUES ('\x45534b', '\xc48c45534bc39d', '\xc48c45534bc39d', 'QWIODJOWIEC');

select * from TS;

SELECT NLS_LOWER(COL1)from TS;
SELECT NLS_LOWER(COL2)from TS;
SELECT NLS_LOWER(COL3)from TS;
SELECT NLS_LOWER(COL4)from TS;
