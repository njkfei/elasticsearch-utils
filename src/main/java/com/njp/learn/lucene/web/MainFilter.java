package com.njp.learn.lucene.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

public class MainFilter implements Filter {

	public void init(FilterConfig arg0) throws ServletException {
		System.out.println("Filter 初始化");
	}

	public void doFilter(ServletRequest req, ServletResponse res,
			FilterChain chain) throws IOException, ServletException {
		HttpServletRequest request = (HttpServletRequest)req;
		System.out.println("拦截 URI="+request.getRequestURI());
		 long before = System.currentTimeMillis(); 
		 System.out.println("开始过滤... "); 
		 
		 // 设置响应内容类型
		 res.setContentType("text/html");
		 res.setCharacterEncoding("utf-8");
		 
		chain.doFilter(req, res);
		 long after = System.currentTimeMillis(); 
	        // 记录日志 
		 System.out.println("过滤结束"); 
	        // 再次记录日志 
		 System.out.println(" 请求被定位到" + ((HttpServletRequest) request).getRequestURI() 
	                + "所花的时间为: " + (after - before)); 
		 
			

	}

	public void destroy() {
		System.out.println("Filter 结束");
	}
}
