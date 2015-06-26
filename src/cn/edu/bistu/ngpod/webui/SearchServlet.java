package cn.edu.bistu.ngpod.webui;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class SearchServlet
 */
@WebServlet("/search")
public class SearchServlet extends HttpServlet {

	private static final long serialVersionUID = 7240004257927088502L;
	private static final int resultsPerPage = 10;

	/**
	 * @throws IOException
	 * @see HttpServlet#HttpServlet()
	 */
	public SearchServlet() throws IOException {
		super();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		String start = request.getParameter("start");//起始日期
		String end = request.getParameter("end");//结束日期
		String[] checkbox = request.getParameterValues("withWP");//是否包含壁纸
		String order = request.getParameter("order");//页面方向:0表示从起始日期向前，1表示从起始日期向后
		System.out.println("START:"+start);
		System.out.println("END:"+end);
		if(checkbox==null){
			System.out.println("NO Check");
		}else{
			for(String check : checkbox){
				System.out.println("CHECK:"+check);				
			}
		}
		response.getWriter().write("HELLO!");
		response.getWriter().close();
	}
}
