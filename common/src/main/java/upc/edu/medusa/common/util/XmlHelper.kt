package upc.edu.medusa.common.util

import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.io.InputStream
import java.io.StringWriter
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.OutputKeys
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory

/**
 * Created by msi on 2017/5/9.
 */
object XmlHelper {
    /**
     * xml文档转为Document对象
     * @param inputStream   输入流
     */
    fun toDocument(inputStream: InputStream): Document {
        return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream)
    }

    /**
     * Document对象转xml文档
     * @param document
     */
    fun toXml(document: Document): String {
        val transformer = TransformerFactory.newInstance().newTransformer()
        transformer.setOutputProperty(OutputKeys.INDENT, "no")
        val sw = StringWriter()
        transformer.transform(DOMSource(document), StreamResult(sw))
        return sw.toString()
    }

    /**
     * 根据表达式获取指定节点
     * @param expression    表达式
     * @param document      Document对象
     */
    fun getNode(expression: String, document: Document): Node? {
        val path = XPathFactory.newInstance().newXPath()
        val node = path.evaluate(expression, document, XPathConstants.NODE)
        if (node != null) {
            return node as Node
        }
        return null
    }

    /**
     * 根据表达式获取指定节点列表
     * @param expression    表达式
     * @param document      Document对象
     */
    fun getNodeList(expression: String, document: Document): NodeList? {
        val path = XPathFactory.newInstance().newXPath()
        val nodeList = path.evaluate(expression, document, XPathConstants.NODESET)
        if (nodeList != null) {
            return nodeList as NodeList
        }
        return null
    }

    /**
     * 获取指定Node中的属性值
     * @param node      指定Node
     * @param propName  属性名称
     */
    fun getAttrValue(node: Node, propName: String): String? {
        val attr = node.attributes.getNamedItem(propName) ?: return null
        return attr.nodeValue
    }

    /**
     * 根据节点名称获取指定节点的唯一子节点
     * @param node      指定节点
     * @param nodeName  子节点名称
     */
    fun getChildrenSingleNode(node: Node, nodeName: String): Node {
        val nodes = getChildrenNodes(node, nodeName)
        if (nodes.size > 1) {
            throw IllegalArgumentException("当前节点存在多个子节点, 当前节点名称为: ${node.nodeName}, 节点名称为: $nodeName")
        }
        if (nodes.size == 0) {
            throw IllegalArgumentException("当前节点不存在子节点, 当前节点名称为: ${node.nodeName}, 节点名称为: $nodeName")
        }
        return nodes.get(0)
    }

    /**
     * 根据节点名称获取指定节点的唯一子节点
     * @param node      指定节点
     * @param nodeNames  子节点名称集合
     */
    fun getChildrenSingleNodeIn(node: Node, nodeNames: MutableList<String>): Node {
        getChildrenNodes(node).forEach {
            if (nodeNames.contains(it.nodeName)) {
                return it
            }
        }
        throw IllegalArgumentException("当前节点不存在子节点, 当前节点名称为: ${node.nodeName}, 节点集合为: $nodeNames")
    }

    /**
     * 根据节点名称获取指定节点的唯一子节点, 可为空, 不抛出异常
     * @param node      指定节点
     * @param nodeName  子节点名称
     */
    fun getChildrenNode(node: Node, nodeName: String): Node? {
        val nodes = getChildrenNodes(node, nodeName)
        if (nodes.size > 1) {
            throw IllegalArgumentException("当前节点存在多个子节点, 当前节点名称为: ${node.nodeName}, 节点名称为: $nodeName")
        }
        if (nodes.size == 0) {
            return null
        }
        return nodes.get(0)
    }

    /**
     * 根据节点名称获取指定节点的所有节点
     * @param node      指定节点
     * @param nodeName  子节点名称
     */
    fun getChildrenNodes(node: Node?, nodeName: String): List<Node> {
        val list: MutableList<Node> = mutableListOf()
        if (node == null) {
            return list
        }
        val childNodes = node.childNodes
        var i = 0
        while (childNodes != null && i < childNodes.length) {
            val childNode = childNodes.item(i)
            if (!nodeName.isNullOrBlank() && nodeName == childNode.nodeName) {
                list.add(childNode)
            }
            i++
        }
        return list
    }

    /**
     * 获取节点的所有节点
     * @param node      指定节点
     */
    fun getChildrenNodes(node: Node?): List<Node>  {
        val list: MutableList<Node> = mutableListOf()
        if (node == null) {
            return list
        }
        val childNodes = node.childNodes
        var i = 0
        while (childNodes != null && i < childNodes.length) {
            val childNode = childNodes.item(i)
            list.add(childNode)
            i++
        }
        return list
    }
}