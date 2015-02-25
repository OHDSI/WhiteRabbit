/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
 * 
 * This file is part of WhiteRabbit
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.rabbitInAHat;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.Polygon;

public class Arrow implements MappingComponent {

	public static float			thickness		= 5;
	public static int			headThickness	= 15;
	public static Color			color	    	= new Color(128, 128, 128, 128);
	public static Color			sourceColor		= new Color(255, 0, 255, 255);
	public static Color			targetColor		= new Color(128, 255, 0, 255);
	private static BasicStroke	dashed			= new BasicStroke(2.0f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f, new float[] { 10.f }, 0.0f);

	private int					x1;
	private int					y1;
	private int					x2;
	private int					y2;
	private LabeledRectangle	source			= null;
	private LabeledRectangle	target			= null;

	private int					width;
	private int					height;

	private Polygon				polygon;

	private boolean				isSelected		= false;
	private boolean				isVisible		= true;																										;

	public Arrow(LabeledRectangle source) {
		this.source = source;
	}

	public Arrow(int x1, int y1, int x2, int y2) {
		this.x1 = x1;
		this.y1 = y1;
		this.x2 = x2;
		this.y2 = y2;

		this.width = Math.abs(x1 - x2);
		this.height = Math.abs(y1 - y2);
	}

	public Arrow(LabeledRectangle source, LabeledRectangle target) {
		this.source = source;
		this.target = target;
	}

	public int getWidth() {
		return width;
	}

	public int getHeight() {
		return height;
	}

	public LabeledRectangle getSource() {
		return source;
	}

	public void setTargetPoint(Point point) {
		if (point == null) {
			x2 = source.getX() + source.getWidth() + Arrow.headThickness;
			y2 = source.getY() + source.getHeight() / 2;
		} else {
			x2 = point.x;
			y2 = point.y;
		}
	}

	public void paint(Graphics g) {
		if (!isVisible)
			return;
		Graphics2D g2d = (Graphics2D) g;

		if (source != null) {
			x1 = source.getX() + source.getWidth();
			y1 = source.getY() + source.getHeight() / 2;
			width = Math.abs(x1 - x2);
			height = Math.abs(y1 - y2);
		}
		if (target != null) {
			x2 = target.getX();
			y2 = target.getY() + target.getHeight() / 2;
			width = Math.abs(x1 - x2);
			height = Math.abs(y1 - y2);
		}
		int nPoints = 25;
		int[] xPoints = new int[nPoints * 2 + 3];
		int[] yPoints = new int[nPoints * 2 + 3];
		float widthMinHead = getWidth() - headThickness;
		float stepSize = widthMinHead / (float) (nPoints - 1);

		for (int i = 0; i < nPoints; i++) {
			float x = x1 + stepSize * i;
			float y = (float) (y1 + (Math.cos(Math.PI * i / (float) nPoints) / 2d - 0.5) * (y1 - y2));
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

		if (isSelected) {
			g2d.setColor(Color.BLACK);
			g2d.setStroke(dashed);
			g2d.drawPolygon(polygon);
		}
	}

	public Color fillColor() {
		if (source != null && source.isSelected()) {
			return sourceColor;
		} else if (target != null && target.isSelected()) {
			return targetColor;
		} else {
			return color;
		}
	}

	public static void drawArrowHead(Graphics2D g2d, int x, int y) {
		int nPoints = 3;
		int[] xPoints = new int[nPoints];
		int[] yPoints = new int[nPoints];
		xPoints[0] = x - headThickness;
		yPoints[0] = y - headThickness;
		xPoints[1] = x;
		yPoints[1] = y;
		xPoints[2] = x - headThickness;
		yPoints[2] = y + headThickness;
		g2d.setColor(color);
		g2d.fillPolygon(xPoints, yPoints, nPoints);
	}

	public void setTarget(LabeledRectangle target) {
		this.target = target;
	}

	public LabeledRectangle getTarget() {
		return target;
	}

	public boolean isSelected() {
		return isSelected;
	}

	public void setSelected(boolean isSelected) {
		this.isSelected = isSelected;
	}

	public boolean contains(Point point) {
		return polygon.contains(point);
	}

	public void setVisible(boolean value) {
		isVisible = value;
	}
}
