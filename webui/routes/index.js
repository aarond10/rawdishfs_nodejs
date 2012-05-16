
/*
 * GET home page.
 */

exports.index = function(req, res){
  res.render('index', { title: 'RawDish Console', stats: 'dummystats' })
};
