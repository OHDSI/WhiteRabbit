package org.ohdsi.rabbitInAHat;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Polygon;
import java.util.ArrayList;
import java.util.List;

public class ManyToOneArrow extends Arrow {

	private List<LabeledRectangle>	sources		= new ArrayList<LabeledRectangle>();
	private boolean					isVisible	= true;

	public ManyToOneArrow(List<LabeledRectangle> sources, LabeledRectangle target) {
		super(null, target);
		this.sources = sources;
		setTarget(target);
	}

	public List<LabeledRectangle> getSources() {
		return sources;
	}

	public void setSources(List<LabeledRectangle> sources) {
		this.sources = sources;
	}

	public void paint(Graphics g) {
		if (!isVisible)
			return;
		Graphics2D g2d = (Graphics2D) g;
		int mergeY = 0;
		for (LabeledRectangle source : sources)
			mergeY += source.getY() + source.getHeight() / 2;
		mergeY /= sources.size();
		for (LabeledRectangle source : sources) {
			x1 = source.getX() + source.getWidth();
			y1 = source.getY() + source.getHeight() / 2;
			width = Math.abs(x1 - x2);
			height = Math.abs(y1 - y2);
			x2 = getTarget().getX();
			y2 = getTarget().getY() + getTarget().getHeight() / 2;
			width = Math.abs(x1 - x2);
			height = Math.abs(y1 - y2);
			int nPoints = 25;
			int[] xPoints = new int[nPoints * 2 + 3];
			int[] yPoints = new int[nPoints * 2 + 3];
			float widthMinHead = getWidth() - headThickness;
			float stepSize = widthMinHead / (float) (nPoints - 1);
			for (int i = 0; i < nPoints / 2; i++) {
				float x = x1 + stepSize * i;
				float y = (float) (y1 + (Math.cos(Math.PI * i / (float) (nPoints / 2)) / 2f - 0.5) * (y1 - mergeY));
				xPoints[i] = Math.round(x);
				yPoints[i] = Math.round(y - thickness);
				xPoints[nPoints * 2 + 3 - i - 1] = Math.round(x);
				yPoints[nPoints * 2 + 3 - i - 1] = Math.round(y + thickness);
			}
			for (int i = nPoints / 2; i < nPoints; i++) {
				float x = x1 + stepSize * i;
				float y = (float) (mergeY + (Math.cos(Math.PI * (i - nPoints / 2d) / (float) (nPoints / 2)) / 2f - 0.5) * (mergeY - y2));
				xPoints[i] = Math.round(x);
				yPoints[i] = Math.round(y - thickness);
				xPoints[nPoints * 2 + 3 - i - 1] = Math.round(x);
				yPoints[nPoints * 2 + 3 - i - 1] = Math.round(y + thickness);
			}
			xPoints[nPoints] = x2 - headThickness;
			yPoints[nPoints] = y2 - headThickness;
			xPoints[nPoints + 1] = x2;
			yPoints[nPoints + 1] = y2;
			xPoints[nPoints + 2] = x2 - headThickness;
			yPoints[nPoints + 2] = y2 + headThickness;
			polygon = new Polygon(xPoints, yPoints, nPoints * 2 + 3);

			g2d.setColor(fillColor());
			g2d.fillPolygon(polygon);

			if (getIsSelected()) {
				g2d.setColor(Color.BLACK);
				g2d.setStroke(dashed);
				g2d.drawPolygon(polygon);
			}
		}
	}
}