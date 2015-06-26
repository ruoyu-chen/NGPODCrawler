<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>国家地理每日一图检索系统</title>
</head>
<body>
	<form action="search" method="post">
		起始时间：<input type="text" name="start" />
		结束时间：<input type="text" name="end" />
		包含壁纸：<input type="checkbox" name="withWP" value="1">
		<!-- 默认选择第一页 -->
		<input type="hidden" name="page" value="1">
		<input type="submit" value="submit" title="提交" />
	</form>
	
	<jsp:useBean id="POD" class="cn.edu.bistu.ngpod.crawler.Ngpod" scope="request"/>
	<h2><jsp:getProperty name="POD" property="title"/></h2>
  <p>作者：<jsp:getProperty name="POD" property="credit"/></p>
	<p>描述：<jsp:getProperty name="POD" property="desc"/></p>
	<p>发布日期：<jsp:getProperty name="POD" property="pubTime"/></p>
	<p><img alt="照片" src="photos/<jsp:getProperty name="POD" property="photo"/>"/></p>
</body>
</html>