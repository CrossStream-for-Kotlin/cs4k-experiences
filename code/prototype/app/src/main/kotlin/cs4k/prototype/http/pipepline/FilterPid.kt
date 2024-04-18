package cs4k.prototype.http.pipepline

import org.springframework.stereotype.Component;

import jakarta.servlet.FilterChain
import jakarta.servlet.ServletRequest
import jakarta.servlet.ServletResponse
import jakarta.servlet.http.HttpFilter
import jakarta.servlet.http.HttpServletRequest
import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory;

@Component
class LoggingFilter : HttpFilter() {

    companion object {
        private val logger = LoggerFactory.getLogger(LoggingFilter::class.java)
    }

    override fun doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
        val runtimeBean = ManagementFactory.getRuntimeMXBean()
        val pidHostInfo = runtimeBean.name.split("@")
        val pid = pidHostInfo[0]
        val hostName = pidHostInfo.getOrElse(1) { "Indisponível" }

        if (request is HttpServletRequest) {
            logger.info("Instância: $hostName, PID: $pid, Request: ${request.requestURI}")
        }
        chain.doFilter(request, response)
    }
}