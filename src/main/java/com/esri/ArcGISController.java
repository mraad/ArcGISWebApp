package com.esri;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.search.Attribute;
import net.sf.ehcache.search.Query;
import net.sf.ehcache.search.Result;
import net.sf.ehcache.search.Results;
import net.sf.ehcache.search.aggregator.Aggregators;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.terracotta.license.util.IOUtils;

import javax.annotation.PostConstruct;
import javax.imageio.ImageIO;
import javax.servlet.http.HttpServletResponse;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.awt.image.ConvolveOp;
import java.awt.image.Kernel;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
@Controller
public class ArcGISController implements ResourceLoaderAware
{
    private final class Range
    {
        double lo = Double.POSITIVE_INFINITY;
        double hi = Double.NEGATIVE_INFINITY;
        double dd;
    }

    private final Log m_log = LogFactory.getLog(ArcGISController.class);

    private ResourceLoader m_resourceLoader;

    private ByteArrayOutputStream m_byteArrayOutputStream = new ByteArrayOutputStream();

    private ConvolveOp m_op;

    private Pattern m_patternEqual;
    private Pattern m_patternComma;
    private Pattern m_patternSemiColon;

    @Autowired
    public CacheManager m_cacheManager;

    @Value("${cache.name}")
    private String m_cacheName;

    @Value("${geohash.xofs}")
    private double m_xofs;

    @Value("${geohash.yofs}")
    private double m_yofs;

    @Value("${geohash.cell}")
    private double m_cell;

    public ArcGISController()
    {
    }

    @Override
    public void setResourceLoader(final ResourceLoader resourceLoader)
    {
        m_resourceLoader = resourceLoader;
    }

    @PostConstruct
    public void postConstruct() throws IOException
    {
        m_patternEqual = Pattern.compile("=");
        m_patternComma = Pattern.compile(",");
        m_patternSemiColon = Pattern.compile(";");

        final Kernel kernel = new Kernel(3, 3,
                new float[]{
                        1f / 9f, 1f / 9f, 1f / 9f,
                        1f / 9f, 1f / 9f, 1f / 9f,
                        1f / 9f, 1f / 9f, 1f / 9f});
        m_op = new ConvolveOp(kernel);

        final Resource resource = m_resourceLoader.getResource("classpath:/InfoUSA.json");
        final InputStream inputStream = resource.getInputStream();
        if (inputStream != null)
        {
            try
            {
                IOUtils.copy(inputStream, m_byteArrayOutputStream);
            }
            finally
            {
                inputStream.close();
            }
        }

        m_xofs = WebMercator.longitudeToX(m_xofs);
        m_yofs = WebMercator.latitudeToY(m_yofs);
    }

    @RequestMapping(value = "/rest/services/InfoUSA/MapServer", method = RequestMethod.GET)
    public void doMapServer(
            final HttpServletResponse response
    ) throws IOException
    {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json");
        response.setContentLength(m_byteArrayOutputStream.size());
        m_byteArrayOutputStream.writeTo(response.getOutputStream());
        response.getOutputStream().flush();
    }

    @RequestMapping(value = "/rest/services/InfoUSA/MapServer/export", method = {RequestMethod.GET, RequestMethod.POST})
    public void doExport(
            @RequestParam("bbox") final String bbox,
            @RequestParam(value = "size", required = false) final String size,
            @RequestParam(value = "layerDefs", required = false) final String layerDefs,
            @RequestParam(value = "transparent", required = false) final String transparent,
            final HttpServletResponse response
    ) throws IOException
    {
        double xmin = -1.0, ymin = -1.0, xmax = 1.0, ymax = 1.0;
        if (bbox != null && bbox.length() > 0)
        {
            final String[] tokens = m_patternComma.split(bbox);
            if (tokens.length == 4)
            {
                xmin = Double.parseDouble(tokens[0]);
                ymin = Double.parseDouble(tokens[1]);
                xmax = Double.parseDouble(tokens[2]);
                ymax = Double.parseDouble(tokens[3]);
            }
            else
            {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "bbox is not in the form xmin,ymin,xmax,ymax");
                return;
            }
        }
        else
        {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "bbox is null or empty");
            return;
        }

        final double xdel = xmax - xmin;
        final double ydel = ymax - ymin;

        int imageWidth = 400;
        int imageHeight = 400;
        if (size != null && size.length() > 0)
        {
            final String[] tokens = m_patternComma.split(size);
            if (tokens.length == 2)
            {
                imageWidth = Integer.parseInt(tokens[0], 10);
                imageHeight = Integer.parseInt(tokens[1], 10);
            }
            else
            {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "size is not in the form width,height");
                return;
            }
        }
        String where = null;
        double lo = Double.NaN;
        double hi = Double.NaN;
        int[] ramp = null;
        if (layerDefs != null)
        {
            final String[] tokens = m_patternSemiColon.split(layerDefs);
            for (final String token : tokens)
            {
                final String[] keyval = m_patternEqual.split(token.substring(2));
                if (keyval.length == 2)
                {
                    final String key = keyval[0];
                    final String val = keyval[1];
                    if ("lo".equalsIgnoreCase(key))
                    {
                        lo = "NaN".equalsIgnoreCase(val) ? Double.NaN : Double.parseDouble(val);
                    }
                    else if ("hi".equalsIgnoreCase(key))
                    {
                        hi = "NaN".equalsIgnoreCase(val) ? Double.NaN : Double.parseDouble(val);
                    }
                    else if ("ramp".equalsIgnoreCase(key))
                    {
                        ramp = parseRamp(val);
                    }
                    else if ("where".equalsIgnoreCase(key))
                    {
                        where = val;
                    }
                }
            }
        }
        if (ramp == null)
        {
            ramp = new int[]{0xFFFFFF, 0x000000};
        }

        final Range range = new Range();
        final Map<Long, Double> map = query(where, xmin, ymin, xmax, ymax, range);
        if (!Double.isNaN(lo))
        {
            range.lo = lo;
        }
        if (!Double.isNaN(hi))
        {
            range.hi = hi;
        }
        range.dd = range.hi - range.lo;

        final int typeIntRgb = "true".equalsIgnoreCase(transparent) ? BufferedImage.TYPE_INT_ARGB : BufferedImage.TYPE_INT_RGB;
        BufferedImage bufferedImage = new BufferedImage(imageWidth, imageHeight, typeIntRgb);
        final Graphics2D graphics = bufferedImage.createGraphics();
        try
        {
            graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

            graphics.setBackground(new Color(0, 0, 0, 0));

            drawCells(graphics, imageWidth, imageHeight, xmin, ymin, xdel, ydel, range, ramp, map.entrySet());
        }
        finally
        {
            graphics.dispose();
        }

        // bufferedImage = m_op.filter(bufferedImage, null);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(10 * 1024);
        ImageIO.write(bufferedImage, "PNG", baos);

        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("image/png");
        response.setContentLength(baos.size());
        baos.writeTo(response.getOutputStream());
        response.getOutputStream().flush();
    }

    private int[] parseRamp(final String val)
    {
        int[] ramp = null;
        final String[] tokens = m_patternComma.split(val);
        final int length = tokens.length;
        if (length > 1)
        {
            ramp = new int[length];
            for (int i = 0; i < length; i++)
            {
                ramp[i] = Integer.parseInt(tokens[i], 16);
            }
        }
        return ramp;
    }

    private void drawCells(
            final Graphics2D graphics,
            final int imageWidth,
            final int imageHeight,
            final double xmin,
            final double ymin,
            final double xdel,
            final double ydel,
            final Range range,
            final int[] ramp,
            final Set<Map.Entry<Long, Double>> set)
    {
        final int cellPixel = Math.max(1, (int) Math.ceil(imageWidth * m_cell / xdel));
        final int cellPixel2 = cellPixel / 2;

        final Color[] colors = new Color[256];

        for (final Map.Entry<Long, Double> entry : set)
        {
            final long key = entry.getKey();
            final double val = Math.min(range.hi, entry.getValue());

            if (val < range.lo)
            {
                continue;
            }

            final long cx = key & 0x7FFFL;
            final long cy = (key >> 16) & 0x7FFFL;
            final double wx = toWorld(cx, m_xofs, m_cell);
            final double wy = toWorld(cy, m_yofs, m_cell);
            final int px = toPixel(wx, imageWidth, xmin, xdel) - cellPixel2;
            final int py = imageHeight - toPixel(wy, imageHeight, ymin, ydel) - cellPixel2;

            final Color color = toColor(val, range, ramp, colors); // TODO - Create interface to handle zero range.dd

            graphics.setColor(color);
            graphics.fillRect(px, py, cellPixel, cellPixel);
        }

        // graphics.setColor(Color.RED);
        // graphics.drawRect(0, 0, imageWidth - 1, imageHeight - 1);
    }

    private Color toColor(
            final double val,
            final Range range,
            final int[] ramp,
            final Color[] colors)
    {
        final double fac = (val - range.lo) / range.dd;
        final int index = (int) (255 * fac);
        final Color color = colors[index];
        if (color != null)
        {
            return color;
        }
        final double offset = (ramp.length - 1) * fac;
        final int min = (int) Math.floor(offset);
        final int max = (int) Math.ceil(offset);
        return colors[index] = interpolate(ramp[min], ramp[max], offset - min);
    }

    private Color interpolate(
            final int min,
            final int max,
            final double ofs)
    {
        final int minb = min & 0xFF;
        final int ming = (min >> 8) & 0xFF;
        final int minr = (min >> 16) & 0xFF;
        final int maxb = max & 0xFF;
        final int maxg = (max >> 8) & 0xFF;
        final int maxr = (max >> 16) & 0xFF;
        final int r = (int) (minr + ofs * (maxr - minr));
        final int g = (int) (ming + ofs * (maxg - ming));
        final int b = (int) (minb + ofs * (maxb - minb));
        return new Color(r, g, b); // TODO - pass in transparency !
    }

    private int toPixel(
            final double n,
            final int size,
            final double min,
            final double del)
    {
        return (int) Math.floor(size * (n - min) / del);
    }

    private double toWorld(
            final long n,
            final double ofs,
            final double cell)
    {
        return n * cell + ofs;
    }

    private Pattern m_and = Pattern.compile("\\s+and\\s+");
    private Pattern m_nume = Pattern.compile("\\s*([a-zA-Z]+)\\s+(eq|ne|le|lt|gt|ge)\\s+(\\d+)\\s*");
    private Pattern m_real = Pattern.compile("\\s*([a-zA-Z]+)\\s+(eq|ne|le|lt|gt|ge)\\s+(\\d+\\.\\d*)\\s*");
    private Pattern m_text = Pattern.compile("\\s*([a-zA-Z]+)\\s+(eq|ne|le|lt|gt|ge)\\s+'([^']*)'\\s*");

    private void addCriteria(
            final Cache cache,
            final Query query,
            final String where)
    {
        if (where != null && where.length() > 0)
        {
            final String[] tokens = m_and.split(where);
            if (tokens.length > 0)
            {
                for (final String token : tokens)
                {
                    match(cache, query, token);
                }
            }
            else
            {
                match(cache, query, where);
            }
        }
    }

    private boolean match(
            final Cache cache,
            final Query query,
            final String token
    )
    {
        if (matchText(cache, query, token))
        {
            return true;
        }
        if (matchNume(cache, query, token))
        {
            return true;
        }
        return matchReal(cache, query, token);
    }

    private boolean matchText(
            final Cache cache,
            final Query query,
            final String token)
    {
        final Matcher matcher = m_text.matcher(token);
        final boolean matches = matcher.matches();
        if (matches)
        {
            final String lhs = matcher.group(1);
            final String op = matcher.group(2);
            final String rhs = matcher.group(3);
            final Attribute<String> attribute = cache.getSearchAttribute(lhs);
            queryAddCriteria(query, attribute, op, rhs);
        }
        return matches;
    }

    private boolean matchNume(
            final Cache cache,
            final Query query,
            final String token)
    {
        final Matcher matcher = m_nume.matcher(token);
        final boolean matches = matcher.matches();
        if (matches)
        {
            final String lhs = matcher.group(1);
            final String op = matcher.group(2);
            final long rhs = Long.parseLong(matcher.group(3));
            final Attribute<Long> attribute = cache.getSearchAttribute(lhs);
            queryAddCriteria(query, attribute, op, rhs);
        }
        return matches;
    }

    private boolean matchReal(
            final Cache cache,
            final Query query,
            final String token)
    {
        final Matcher matcher = m_real.matcher(token);
        final boolean matches = matcher.matches();
        if (matches)
        {
            final String lhs = matcher.group(1);
            final String op = matcher.group(2);
            final double rhs = Double.parseDouble(matcher.group(3));
            final Attribute<Double> attribute = cache.getSearchAttribute(lhs);
            queryAddCriteria(query, attribute, op, rhs);
        }
        return matches;
    }

    private void queryAddCriteria(
            final Query query,
            final Attribute attribute,
            final String op,
            final Object val)
    {
        if ("eq".equalsIgnoreCase(op))
        {
            query.addCriteria(attribute.eq(val));
        }
        else if ("ne".equalsIgnoreCase(op))
        {
            query.addCriteria(attribute.ne(val));
        }
        else if ("le".equalsIgnoreCase(op))
        {
            query.addCriteria(attribute.le(val));
        }
        else if ("lt".equalsIgnoreCase(op))
        {
            query.addCriteria(attribute.lt(val));
        }
        else if ("gt".equalsIgnoreCase(op))
        {
            query.addCriteria(attribute.gt(val));
        }
        else if ("ge".equalsIgnoreCase(op))
        {
            query.addCriteria(attribute.ge(val));
        }
    }

    private Map<Long, Double> query(
            final String where,
            final double xmin,
            final double ymin,
            final double xmax,
            final double ymax,
            final Range range)
    {
        final Map<Long, Double> map = new HashMap<Long, Double>();

        final Cache cache = m_cacheManager.getCache(m_cacheName);

        final Attribute<Long> attrGeoHash = cache.getSearchAttribute("geohash");
        final Attribute<Double> attrX = cache.getSearchAttribute("x");
        final Attribute<Double> attrY = cache.getSearchAttribute("y");

        final Query query = cache.createQuery().
                includeAttribute(attrGeoHash).
                includeAggregator(Aggregators.count()).
                addCriteria(attrX.between(xmin - m_cell, xmax + m_cell)).
                addCriteria(attrY.between(ymin - m_cell, ymax + m_cell)).
                addGroupBy(attrGeoHash);

        addCriteria(cache, query, where);

        final Results results = query.execute();
        final List<Result> all = results.all();
        final int size = all.size();
        for (final Result result : all)
        {
            final long geoHash = result.getAttribute(attrGeoHash);
            final List aggregatorsList = result.getAggregatorResults();
            final Integer count = (Integer) aggregatorsList.get(0);
            final double v = count.doubleValue();
            if (v < range.lo)
            {
                range.lo = v;
            }
            if (v > range.hi)
            {
                range.hi = v;
            }
            map.put(geoHash, v);
        }

        m_log.info(String.format("size = %d lo = %.1f hi = %.1f", size, range.lo, range.hi));

        return map;
    }

}
