package com.njp.learn.lucene.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.njp.learn.lucene.es1.SearchUtil;
/**
 * 最简单的Servlet
 * @author niejinping
 */
public class MainServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

/*	@Override
	protected void service(HttpServletRequest req, HttpServletResponse res)
			throws ServletException, IOException {
		res.getWriter().println("Hello World!");
	}*/

	 public void doGet(HttpServletRequest request,
             HttpServletResponse response)
     throws ServletException, IOException{
/*		 	User user = new User(1,"niejinping");
		 	String json = new Gson().toJson(user);
			// 设置响应内容类型
			response.setContentType("text/html");
			response.setCharacterEncoding("utf-8");
			response.setHeader("cookee","mycooki");
			//response.setStatus(404);
			Cookie cookie = new Cookie("cookie1", "cookie1");
			response.addCookie(cookie);
			response.addIntHeader("h1", 111);
			response.addHeader("h2", "header2");
			// response.sendRedirect("localhost:8080/hello/index.html");
			
			// 实际的逻辑是在这里
			PrintWriter out = response.getWriter();
			out.println(json);*/
		// System.out.println("处理URI="+request.getRequestURI());
		// 设置响应内容类型
	      //response.setContentType("text/html");
	      //response.setContentType("utf-8");
	      //response.setCharacterEncoding("utf-8");
		 
/*			Cookie cookie = new Cookie("cookie1", "cookie1");
			cookie.setMaxAge(3600*12);
			response.addCookie(cookie);
			
			Cookie cookie2 = new Cookie("cookie2", "cookie2");
			cookie2.setMaxAge(3600*24);
			response.addCookie(cookie2);
	      response.setHeader("Content-Type", "text/html;charset=UTF-8");*/
	      
/*	      PrintWriter out = response.getWriter();
		  String title = "使用 GET 方法读取表单数据";
	      String docType =
	      "<!doctype html public \"-//w3c//dtd html 4.0 " +
	      "transitional//en\">\n";
	      out.println(docType +
	                "<html>\n" +
	                "<head><title>" + title + "</title></head>\n" +
	                "<body bgcolor=\"#f0f0f0\">\n" +
	                "<h1 align=\"center\">" + title + "</h1>\n" +
	                "<ul>\n" +
	                "  <li><b>名字</b>："
	                + request.getParameter("first_name") + "\n" +
	                "  <li><b>姓氏</b>："
	                + request.getParameter("last_name") + "\n" +
	                "</ul>\n" +
	                "</body></html>");*/
		 
			StringBuilder sb = new StringBuilder();
			
			sb.append("\n requestQueryString : ");
			sb.append(request.getQueryString());
			
			Map map = request.getParameterMap();
			Set<String> keySet = map.keySet();
			String query = "聂";
			for (String key : keySet) {
				
				if(key.equalsIgnoreCase("q")){
				
					String[] values = (String[]) map.get(key);
					
					query = values[0];
					
					
				}
			}
			
			Object json = SearchUtil.StringSearch(query);
			PrintWriter out = response.getWriter();
			out.println(json);
	  }
	 
/*	 1. 获得客户机信息
	    getRequestURL方法返回客户端发出请求时的完整URL。
	    getRequestURI方法返回请求行中的资源名部分。
	    getQueryString 方法返回请求行中的参数部分。
	    getRemoteAddr方法返回发出请求的客户机的IP地址 
	    getRemoteHost方法返回发出请求的客户机的完整主机名
	    getRemotePort方法返回客户机所使用的网络端口号
	    getLocalAddr方法返回WEB服务器的IP地址。
	    getLocalName方法返回WEB服务器的主机名 
	    getMethod得到客户机请求方式
	 2.获得客户机请求头 
	    getHeader(string name)方法 
	    getHeaders(String name)方法 
	    getHeaderNames方法 

	 3. 获得客户机请求参数(客户端提交的数据)
	    getParameter(name)方法
	    getParameterValues（String name）方法
	    getParameterNames方法 
	    getParameterMap方法*/
	 public void doPost(HttpServletRequest request,
             HttpServletResponse response)
     throws ServletException, IOException{
		StringBuilder sb = new StringBuilder();
		
		sb.append("characterencoding : ");
		sb.append(request.getCharacterEncoding()); 
		
		sb.append("\nauthtype:");
		sb.append(request.getAuthType());
		
		sb.append("\ngetContentLength : ");
		sb.append(request.getContentLength());
		
		sb.append("\ngetContextPath : ");
		sb.append(request.getContextPath());
		
		sb.append("\ngetLocalAddr:");
		sb.append(request.getLocalAddr());
		
		sb.append("\ngetLocalName");
		sb.append(request.getLocalName());
		
		sb.append("\n requesturl : ");
		sb.append(request.getRequestURL().toString());
		
		sb.append("\n requestURI :");
		sb.append(request.getRequestURI());
		
		sb.append("\n requestQueryString : ");
		sb.append(request.getQueryString());
		
		sb.append("\n q1 =");
		sb.append(request.getParameter("q1"));
		
		sb.append("\n q4 =");
		sb.append(request.getParameter("q4"));
		
		sb.append("\n remotehost");
		sb.append(request.getRemoteHost());
		
		sb.append("\n method :");
		sb.append(request.getMethod());
		
		sb.append("\n header : h1 = ");
		sb.append(request.getHeader("h1"));
		
		sb.append("\n queryMap : ");
		//得到请求的参数Map，注意map的value是String数组类型  
		Map map = request.getParameterMap();
		Set<String> keySet = map.keySet();
		for (String key : keySet) {
			String[] values = (String[]) map.get(key);
			for (String value : values) {
				sb.append(key + "=" + value + " \t");
			}
		}
		
		for (String key : keySet) {
			String[] values = (String[]) map.get(key);
			sb.append("key : \t");
			sb.append(key);
			
			for (String value : values) {
				sb.append( value + " \t");
			}
		}
		
		sb.append("\n getSession : session = ");
		HttpSession session = request.getSession();
		sb.append(session.getId());
		Enumeration en = session.getAttributeNames();
		
		while(en.hasMoreElements()){
			sb.append("\n");
			sb.append(en.nextElement());
		}
		
		session.setAttribute("name", "Njp");
		
		sb.append("\n cookie : ");
		
		for(Cookie item : request.getCookies()){
			sb.append(item.getName() + " _ " +  item.getValue());
			sb.append("\n");
		}
		
		// 设置响应内容类型
		response.setContentType("text/html");
		response.setCharacterEncoding("utf-8");
		
		// 实际的逻辑是在这里
		PrintWriter out = response.getWriter();
		out.println(sb.toString());
		}
}
