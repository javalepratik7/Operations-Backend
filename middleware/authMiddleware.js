const jwt = require('jsonwebtoken');
const db = require('../models');

const authMiddleware = async (req, res, next) => {
  const token = req.headers['authorization'];
  if (!token) return res.status(401).json({ message: 'No token provided' });

  try {
    const decoded = jwt.verify(token.split(' ')[1], process.env.JWT_SECRET || 'your_jwt_secret');
    
    // Fetch user from database to get latest info
    const user = await db.User.findOne({ 
      where: { email: decoded.email },
      attributes: ['id', 'name', 'email', 'role']
    });
    
    if (!user) {
      return res.status(401).json({ message: 'User not found' });
    }
    
    req.user = {
      id: user.id,
      name: user.name,
      email: user.email,
      role: user.role
    };
    
    next();
  } catch (err) {
    console.error('Auth error:', err);
    return res.status(401).json({ message: 'Invalid token' });
  }
};

module.exports = authMiddleware;