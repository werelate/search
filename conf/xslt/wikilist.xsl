<?xml version='1.0' encoding='UTF-8'?>

<xsl:stylesheet version='1.0'
    xmlns:xsl='http://www.w3.org/1999/XSL/Transform'
>

  <xsl:output method="text" media-type="text/plain; charset=UTF-8" encoding="UTF-8"/> 
  
  <xsl:variable name="title" select="concat('WeRelate search results (',response/result/@numFound,' documents)')"/>
  <xsl:variable name="space" select="' '"/>
  
  <xsl:template match='/'>
<xsl:value-of select="$title"/>
<xsl:apply-templates select="response/result/doc"/>
  </xsl:template>
  
  <xsl:template match="doc">
*[[<xsl:value-of select="str[@name='NamespaceStored']"/>:<xsl:value-of select="str[@name='TitleStored']"/>]] <xsl:value-of select="arr[@name='FullnameStored']/str"/><xsl:value-of select="' '"/><xsl:value-of select="str[@name='PersonBirthDateStored']"/><xsl:value-of select="' '"/><xsl:value-of select="str[@name='PersonDeathDateStored']"/><xsl:value-of select="str[@name='MarriageDateStored']"/></xsl:template>

</xsl:stylesheet>
