/*******************************************************************************
 * Copyright 2019 Observational Health Data Sciences and Informatics
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
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.Stroke;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.List;

import javax.swing.event.ChangeListener;

import org.ohdsi.rabbitInAHat.dataModel.MappableItem;

public class LabeledRectangle implements MappingComponent {

	public static int				FONT_SIZE		= 18;

	private static Stroke			stroke			= new BasicStroke(2);
	private static BasicStroke		dashed			= new BasicStroke(2.0f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f, new float[] { 10.f }, 0.0f);
	private List<ChangeListener>	changeListeners	= new ArrayList<ChangeListener>();
	private int						x;
	private int						y;
	private int						width;
	private int						height;
	private MappableItem			item;
	private Color					baseColor;
	private Color					transparentColor;
	private boolean					isVisible		= true;
	private boolean					isSelected		= false;

	public void addChangeListener(ChangeListener x) {
		changeListeners.add(x);
	}

	public void removeChangeListener(ChangeListener x) {
		changeListeners.remove(x);
	}

	public LabeledRectangle(int x, int y, int width, int height, MappableItem item, Color baseColor) {
		this.x = x;
		this.y = y;
		this.width = width;
		this.height = height;		
		this.item = item;
		this.baseColor = baseColor;
		this.transparentColor = new Color(baseColor.getRed(), baseColor.getGreen(), baseColor.getBlue(), 128);
	}

	public boolean isVisible(){
		return isVisible == true;
	}
	
	public int getWidth() {
		return width;
	}

	public int getHeight() {
		return height;
	}

	public void setLocation(int x, int y) {
		this.x = x;
		this.y = y;
	}
	
	public void filter(String searchTerm){
		if (this.getItem().getName().matches(".*(" + searchTerm + ").*") || searchTerm.equals("") ){
			this.setVisible(true);				
		}else{
			this.setVisible(false);
			this.setSelected(false);
		}
	}
	
	public void paint(Graphics g) {
		if (!isVisible)
			return;		
		
		Graphics2D g2d = (Graphics2D) g.create();		
		
		g2d.setColor(transparentColor);
		g2d.fillRect(x, y, width, height);
		if (isSelected) {
			g2d.setColor(Color.BLACK);
			g2d.setStroke(dashed);
		} else {
			g2d.setColor(baseColor);
			g2d.setStroke(stroke);
		}
		g2d.drawRect(x, y, width, height);
		g2d.setColor(Color.BLACK);

		g2d.setFont(new Font("default", Font.PLAIN, FONT_SIZE));
		FontMetrics fm = g2d.getFontMetrics();

		Rectangle2D r = fm.getStringBounds(item.outputName(), g2d);
		if (r.getWidth() >= width) {
			int breakPoint = 0;
			int index = nextBreakPoint(item.outputName(), 0);
			double midPoint = item.outputName().length() / 2d;
			while (index != -1) {
				if (Math.abs(index - midPoint) < Math.abs(breakPoint - midPoint))
					breakPoint = index;
				index = nextBreakPoint(item.outputName(), index + 1);
			}
			if (breakPoint == 0) {
				int textX = (this.getWidth() - (int) r.getWidth()) / 2;
				int textY = (this.getHeight() - (int) r.getHeight()) / 2 + fm.getAscent();
				g2d.drawString(item.outputName(), x + textX, y + textY);
			}
			breakPoint++;
			String line1 = item.outputName().substring(0, breakPoint);
			String line2 = item.outputName().substring(breakPoint);
			r = fm.getStringBounds(line1, g2d);
			int textX = (this.getWidth() - (int) r.getWidth()) / 2;
			int textY = (this.getHeight() / 2 - (int) r.getHeight()) / 2 + fm.getAscent();
			g2d.drawString(line1, x + textX, y + textY);
			r = fm.getStringBounds(line2, g2d);
			textX = (this.getWidth() - (int) r.getWidth()) / 2;
			textY = (int) Math.round(this.getHeight() * 1.5 - (int) r.getHeight()) / 2 + fm.getAscent();
			g2d.drawString(line2, x + textX, y + textY);
		} else {
			int textX = (this.getWidth() - (int) r.getWidth()) / 2;
			int textY = (this.getHeight() - (int) r.getHeight()) / 2 + fm.getAscent();
			g2d.drawString(item.outputName(), x + textX, y + textY);
		}
	}

	private int nextBreakPoint(String string, int start) {
		int index1 = string.indexOf(' ', start);
		int index2 = string.indexOf('_', start);
		if (index1 == -1)
			return index2;
		else if (index2 == -1)
			return index1;
		else
			return Math.min(index1, index2);
	}

	public boolean contains(Point point) {
		return (point.x >= x && point.x <= x + width && point.y >= y && point.y <= y + height);
	}
	
	public boolean contains(Point point, int xOffset, int yOffset) {
		Point p = new Point(point.x + xOffset, point.y + yOffset);
		return contains(p);
	}

	public int getX() {
		return x;
	}

	public int getY() {
		return y;
	}

	public MappableItem getItem() {
		return item;
	}

	public boolean isSelected() {
		return isSelected;
	}

	public void setSelected(boolean isSelected) {
		this.isSelected = isSelected;
	}
	
	public boolean toggleSelected(){
		this.isSelected = !this.isSelected;
		return isSelected;
	}

	public void setVisible(boolean value) {
		isVisible = value;
	}

}
