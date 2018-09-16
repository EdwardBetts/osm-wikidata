from matcher import wikipedia

def test_name_from_html():
    sample = '''<div class="mw-parser-output"><div class="thumb tright"> <div class="thumbinner" style="width:252px;"><a href="/wiki/File:Isaac_Newton_Institute_building.jpg" class="image"><img alt="" src="//upload.wikimedia.org/wikipedia/commons/thumb/1/14/Isaac_Newton_Institute_building.jpg/250px-Isaac_Newton_Institute_building.jpg" width="250" height="139" class="thumbimage" srcset="//upload.wikimedia.org/wikipedia/commons/thumb/1/14/Isaac_Newton_Institute_building.jpg/375px-Isaac_Newton_Institute_building.jpg 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/1/14/Isaac_Newton_Institute_building.jpg/500px-Isaac_Newton_Institute_building.jpg 2x" data-file-width="1184" data-file-height="660" /></a> <div class="thumbcaption"> <div class="magnify"><a href="/wiki/File:Isaac_Newton_Institute_building.jpg" class="internal" title="Enlarge"></a></div> Main building for the Isaac Newton Institute</div> </div> </div> <p><b>The Isaac Newton Institute for Mathematical Sciences</b> is an international research institute for mathematics and its many applications at the <a href="/wiki/University_of_Cambridge" title="University of Cambridge">University of Cambridge</a>. It is named after one of the university's most illustrious figures, the mathematician and natural philosopher <a href="/wiki/Sir_Isaac_Newton" class="mw-redirect" title="Sir Isaac Newton">Sir Isaac Newton</a> and occupies buildings adjacent to the Cambridge <a href="/wiki/Centre_for_Mathematical_Sciences_(Cambridge)" title="Centre for Mathematical Sciences (Cambridge)">Centre for Mathematical Sciences</a>.</p>'''  # noqa: E501
    expect = ['The Isaac Newton Institute for Mathematical Sciences']
    assert wikipedia.html_names(sample) == expect

def test_bullet_list_html():
    sample = '''<p>The <b>Shepherdstown Historic District</b> comprises the historic core of Shepherdstown, West Virginia. The town is the oldest in West Virginia, founded in 1762 as Mecklenburg.</p>
<p>Some of the more significant elements are:</p>
<ul>
<li><b>Baker House</b>, a Federal style brick house with a Roman Revival porch, dating to the 1790s. It was the home of US Representative John Baker.</li>
<li>The <b>Great Western Hotel</b>, owned by Jacob Entler.  Originally a log structure, it was extensively modified in the early 19th century.</li>
<li><b>The Presbyterian Manse</b>, a brick Federal style or Classical Revival house, home of John Kearsley, a prominent local landowner.</li>
<li><b>Trinity Episcopal Rectory</b>, a Federal style house that was a home of John Baker, as well as US Representative Thomas Van Swearingen.</li>
<li>The <b>Lane House</b>, a Federal style house once owned by Harriet Lane, niece and hostess for President James Buchanan.</li>
<li>The <b>Sheetz House</b>, a Federal style house where muskets were manufactured during the American Revolutionary War.</li>
<li>The <b>Old Market House</b>, the town's former market built in 1800, with stepped gable ends.  A second story was added in 1845 by the Odd Fellows with a 999-year lease.  The first floor has been a public library since 1922.</li>
</ul>'''

    expect = ['Shepherdstown Historic District']
    assert wikipedia.html_names(sample) == expect
