package cn.edu.bistu.ngpod.webui;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import cn.edu.bistu.ngpod.crawler.HBaseSearch;
import cn.edu.bistu.ngpod.crawler.Ngpod;

/**
 * Servlet implementation class SearchServlet
 */
@WebServlet("/start")
public class StartServlet extends HttpServlet {

	private static final long serialVersionUID = 7240004257927088502L;

	/**
	 * @throws IOException
	 * @see HttpServlet#HttpServlet()
	 */
	public StartServlet() throws IOException {
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
		HBaseSearch search = HBaseSearch.getInstance();
		Ngpod pod = search.getLatest();
		System.out.println(pod.toString());
		request.setAttribute("POD", pod);
		request.getRequestDispatcher("index.jsp").forward(request, response);
	}
}
